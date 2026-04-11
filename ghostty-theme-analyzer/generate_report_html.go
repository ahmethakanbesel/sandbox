package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Color struct{ R, G, B uint8 }
type OptColor struct {
	Color Color
	Set   bool
}

func parseHexColor(s string) (Color, error) {
	s = strings.TrimSpace(strings.TrimPrefix(s, "#"))
	if len(s) != 6 {
		return Color{}, fmt.Errorf("invalid")
	}
	r, _ := strconv.ParseUint(s[0:2], 16, 8)
	g, _ := strconv.ParseUint(s[2:4], 16, 8)
	b, _ := strconv.ParseUint(s[4:6], 16, 8)
	return Color{uint8(r), uint8(g), uint8(b)}, nil
}

func linearize(c uint8) float64 {
	s := float64(c) / 255.0
	if s <= 0.04045 {
		return s / 12.92
	}
	return math.Pow((s+0.055)/1.055, 2.4)
}

func luminance(c Color) float64 {
	return 0.2126*linearize(c.R) + 0.7152*linearize(c.G) + 0.0722*linearize(c.B)
}

func contrast(c1, c2 Color) float64 {
	l1, l2 := luminance(c1), luminance(c2)
	if l1 < l2 {
		l1, l2 = l2, l1
	}
	return (l1 + 0.05) / (l2 + 0.05)
}

func hex(c Color) string { return fmt.Sprintf("#%02x%02x%02x", c.R, c.G, c.B) }

// APCA-W3 0.0.98G-4g — Accessible Perceptual Contrast Algorithm
// Reference: https://github.com/Myndex/apca-w3
func apcaLinearize(c uint8) float64 {
	return math.Pow(float64(c)/255.0, 2.4) // simple gamma, not sRGB piecewise
}

func apcaLuminance(c Color) float64 {
	return 0.2126729*apcaLinearize(c.R) + 0.7151522*apcaLinearize(c.G) + 0.0721750*apcaLinearize(c.B)
}

func apcaSoftClamp(y float64) float64 {
	const blkThrs = 0.022
	const blkClmp = 1.414
	if y >= blkThrs {
		return y
	}
	return y + math.Pow(blkThrs-y, blkClmp)
}

// apcaContrast returns the APCA Lc value for text on background.
// Positive = dark text on light bg. Negative = light text on dark bg.
func apcaContrast(text, bg Color) float64 {
	txtY := apcaSoftClamp(apcaLuminance(text))
	bgY := apcaSoftClamp(apcaLuminance(bg))

	if math.Abs(bgY-txtY) < 0.0005 {
		return 0
	}

	var sapc, lc float64
	if bgY > txtY {
		// Normal polarity: dark text on light bg
		sapc = (math.Pow(bgY, 0.56) - math.Pow(txtY, 0.57)) * 1.14
		if sapc < 0.1 {
			return 0
		}
		lc = (sapc - 0.027) * 100
	} else {
		// Reverse polarity: light text on dark bg
		sapc = (math.Pow(bgY, 0.65) - math.Pow(txtY, 0.62)) * 1.14
		if sapc > -0.1 {
			return 0
		}
		lc = (sapc + 0.027) * 100
	}
	return math.Round(lc*10) / 10
}

// sRGB → CIE XYZ (D65)
func srgbToXYZ(c Color) (X, Y, Z float64) {
	r, g, b := linearize(c.R), linearize(c.G), linearize(c.B)
	X = 0.4124564*r + 0.3575761*g + 0.1804375*b
	Y = 0.2126729*r + 0.7151522*g + 0.0721750*b
	Z = 0.0193339*r + 0.1191920*g + 0.9503041*b
	return
}

// CIE XYZ → CIELAB (D65 white: 0.95047, 1.0, 1.08883)
func xyzToLab(X, Y, Z float64) (L, a, b float64) {
	const Xn, Yn, Zn = 0.95047, 1.0, 1.08883
	f := func(t float64) float64 {
		const delta = 6.0 / 29.0
		if t > delta*delta*delta {
			return math.Cbrt(t)
		}
		return t/(3*delta*delta) + 4.0/29.0
	}
	fx, fy, fz := f(X/Xn), f(Y/Yn), f(Z/Zn)
	L = 116*fy - 16
	a = 500 * (fx - fy)
	b = 200 * (fy - fz)
	return
}

func colorToLab(c Color) (L, a, b float64) {
	X, Y, Z := srgbToXYZ(c)
	return xyzToLab(X, Y, Z)
}

// CIE76 color difference
func deltaE76(c1, c2 Color) float64 {
	L1, a1, b1 := colorToLab(c1)
	L2, a2, b2 := colorToLab(c2)
	return math.Sqrt((L1-L2)*(L1-L2) + (a1-a2)*(a1-a2) + (b1-b2)*(b1-b2))
}

// Blue light index: fraction of blue in linearized RGB (0–1, lower = warmer)
func blueLightIndex(c Color) float64 {
	r, g, b := linearize(c.R), linearize(c.G), linearize(c.B)
	sum := r + g + b
	if sum < 1e-10 {
		return 0.333
	}
	return b / sum
}

// Correlated Color Temperature via McCamy's approximation
func colorTempCCT(c Color) float64 {
	X, Y, Z := srgbToXYZ(c)
	sum := X + Y + Z
	if sum < 1e-10 {
		return 0
	}
	x, y := X/sum, Y/sum
	if math.Abs(0.1858-y) < 1e-10 {
		return 0
	}
	n := (x - 0.3320) / (0.1858 - y)
	cct := 449*n*n*n + 3525*n*n + 6823.3*n + 5520.33
	if cct < 1000 {
		return 1000
	}
	if cct > 25000 {
		return 25000
	}
	return cct
}

// Minimum pairwise deltaE across all 16 palette colors
func paletteMinDeltaE(palette [16]Color) float64 {
	minDE := math.MaxFloat64
	for i := 0; i < 16; i++ {
		for j := i + 1; j < 16; j++ {
			de := deltaE76(palette[i], palette[j])
			if de < minDE {
				minDE = de
			}
		}
	}
	return minDE
}

// Circular standard deviation of hue angles in CIELAB
func paletteHueStdDev(palette [16]Color) float64 {
	var sumSin, sumCos float64
	for i := 0; i < 16; i++ {
		_, a, b := colorToLab(palette[i])
		h := math.Atan2(b, a)
		sumSin += math.Sin(h)
		sumCos += math.Cos(h)
	}
	sumSin /= 16
	sumCos /= 16
	R := math.Sqrt(sumSin*sumSin + sumCos*sumCos)
	if R >= 1.0 {
		return 0
	}
	if R < 1e-10 {
		return 180
	}
	return math.Sqrt(-2*math.Log(R)) * 180 / math.Pi
}

func wcagLevel(r float64) string {
	switch {
	case r >= 7.0:
		return "AAA"
	case r >= 4.5:
		return "AA"
	case r >= 3.0:
		return "AA-Large"
	default:
		return "Fail"
	}
}

// Short JSON keys to minimize payload (~40% smaller).
// Mapping: n=name s=score b=bg f=fg fc=fgContrast fl=fgLevel
// p=palette a=aaCount A=aaaCount c=cursor ct=cursorTxt
// sl=selection sd=selDist h=hex r=contrast(ratio) l=level
// x=fg(pair) y=bg(pair) nt=nonText
type JSONTheme struct {
	Name       string    `json:"n"`
	Score      float64   `json:"s"`
	BG         string    `json:"b"`
	FG         string    `json:"f"`
	FGContrast float64   `json:"fc"`
	FGLevel    string    `json:"fl"`
	Palette    [16]PalC  `json:"p"`
	AACount    int       `json:"a"`
	AAACount   int       `json:"A"`
	Cursor     *PairInfo `json:"c,omitempty"`
	CursorTxt  *PairInfo `json:"ct,omitempty"`
	Selection  *PairInfo `json:"sl,omitempty"`
	SelDist    *PairInfo `json:"sd,omitempty"`
	BlueLight  float64  `json:"bli"`
	CCT        float64  `json:"cct"`
	MinDeltaE  float64  `json:"pde"`
	HueStdDev  float64  `json:"phs"`
	APCAFgBg   float64  `json:"alc"`    // APCA Lc for FG on BG
	APCAPass75 int      `json:"ap75"`   // palette colors with |Lc| >= 75 (body text)
	APCAPass60 int      `json:"ap60"`   // palette colors with |Lc| >= 60
}

type PalC struct {
	Hex      string  `json:"h"`
	Contrast float64 `json:"r"`
	Level    string  `json:"l"`
}

type PairInfo struct {
	FG       string  `json:"x"`
	BG       string  `json:"y"`
	Contrast float64 `json:"r"`
	Level    string  `json:"l"`
	NonText  bool    `json:"nt,omitempty"`
}

var outDir = "."

func main() {
	dir := "/Applications/Ghostty.app/Contents/Resources/ghostty/themes"
	if len(os.Args) > 1 {
		dir = os.Args[1]
	}
	if len(os.Args) > 2 {
		outDir = os.Args[2]
		os.MkdirAll(outDir, 0755)
	}
	entries, _ := os.ReadDir(dir)

	var paths []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		n := e.Name()
		if strings.HasPrefix(n, ".") || strings.HasSuffix(n, ".go") ||
			strings.HasSuffix(n, ".csv") || strings.HasSuffix(n, ".md") ||
			strings.HasSuffix(n, ".html") {
			continue
		}
		paths = append(paths, filepath.Join(dir, n))
	}

	type result struct {
		j   JSONTheme
		err error
	}

	ch := make(chan string, len(paths))
	out := make(chan result, len(paths))
	var wg sync.WaitGroup
	for w := 0; w < runtime.NumCPU(); w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range ch {
				j, err := processTheme(p)
				out <- result{j, err}
			}
		}()
	}
	for _, p := range paths {
		ch <- p
	}
	close(ch)
	go func() { wg.Wait(); close(out) }()

	var themes []JSONTheme
	for r := range out {
		if r.err == nil {
			themes = append(themes, r.j)
		}
	}

	sort.Slice(themes, func(i, j int) bool {
		if themes[i].Score != themes[j].Score {
			return themes[i].Score > themes[j].Score
		}
		return themes[i].Name < themes[j].Name
	})

	data, _ := json.Marshal(themes)
	writeHTML(data)
	writePicker(data)
}

func processTheme(path string) (JSONTheme, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return JSONTheme{}, err
	}

	var bg, fg Color
	var cursor, cursorTxt, selBG, selFG OptColor
	var palette [16]Color
	hasBG, hasFG := false, false

	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		k, v := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
		switch k {
		case "background":
			if c, e := parseHexColor(v); e == nil {
				bg = c; hasBG = true
			}
		case "foreground":
			if c, e := parseHexColor(v); e == nil {
				fg = c; hasFG = true
			}
		case "cursor-color":
			if c, e := parseHexColor(v); e == nil {
				cursor = OptColor{c, true}
			}
		case "cursor-text":
			if c, e := parseHexColor(v); e == nil {
				cursorTxt = OptColor{c, true}
			}
		case "selection-background":
			if c, e := parseHexColor(v); e == nil {
				selBG = OptColor{c, true}
			}
		case "selection-foreground":
			if c, e := parseHexColor(v); e == nil {
				selFG = OptColor{c, true}
			}
		case "palette":
			pp := strings.SplitN(v, "=", 2)
			if len(pp) == 2 {
				if idx, e := strconv.Atoi(strings.TrimSpace(pp[0])); e == nil && idx >= 0 && idx <= 15 {
					if c, e2 := parseHexColor(pp[1]); e2 == nil {
						palette[idx] = c
					}
				}
			}
		}
	}
	if !hasBG || !hasFG {
		return JSONTheme{}, fmt.Errorf("missing bg/fg")
	}

	j := JSONTheme{
		Name:       filepath.Base(path),
		BG:         hex(bg),
		FG:         hex(fg),
		FGContrast: math.Round(contrast(fg, bg)*100) / 100,
		FGLevel:    wcagLevel(contrast(fg, bg)),
	}

	for i := 0; i < 16; i++ {
		cr := contrast(palette[i], bg)
		j.Palette[i] = PalC{hex(palette[i]), math.Round(cr*100) / 100, wcagLevel(cr)}
		if cr >= 4.5 {
			j.AACount++
		}
		if cr >= 7.0 {
			j.AAACount++
		}
	}

	// Extra metrics
	j.BlueLight = math.Round(blueLightIndex(bg)*1000) / 1000
	j.CCT = math.Round(colorTempCCT(bg))
	j.MinDeltaE = math.Round(paletteMinDeltaE(palette)*100) / 100
	j.HueStdDev = math.Round(paletteHueStdDev(palette)*100) / 100

	// APCA
	j.APCAFgBg = apcaContrast(fg, bg)
	for i := 0; i < 16; i++ {
		lc := math.Abs(apcaContrast(palette[i], bg))
		if lc >= 60 {
			j.APCAPass60++
		}
		if lc >= 75 {
			j.APCAPass75++
		}
	}

	if cursor.Set {
		cr := contrast(cursor.Color, bg)
		lv := "Pass"
		if cr < 3.0 {
			lv = "Fail"
		}
		j.Cursor = &PairInfo{hex(cursor.Color), hex(bg), math.Round(cr*100) / 100, lv, true}
	}
	if cursor.Set && cursorTxt.Set {
		cr := contrast(cursorTxt.Color, cursor.Color)
		j.CursorTxt = &PairInfo{hex(cursorTxt.Color), hex(cursor.Color), math.Round(cr*100) / 100, wcagLevel(cr), false}
	}
	if selFG.Set && selBG.Set {
		cr := contrast(selFG.Color, selBG.Color)
		j.Selection = &PairInfo{hex(selFG.Color), hex(selBG.Color), math.Round(cr*100) / 100, wcagLevel(cr), false}
	}
	if selBG.Set {
		cr := contrast(selBG.Color, bg)
		lv := "Pass"
		if cr < 3.0 {
			lv = "Fail"
		}
		j.SelDist = &PairInfo{hex(selBG.Color), hex(bg), math.Round(cr*100) / 100, lv, true}
	}

	// Score
	score := math.Min(contrast(fg, bg)/21.0, 1.0) * 30.0
	score += (float64(j.AACount) / 16.0) * 25.0
	score += (float64(j.AAACount) / 16.0) * 15.0
	if cursor.Set {
		score += math.Min(contrast(cursor.Color, bg)/21.0, 1.0) * 10.0
	} else {
		score += 5.0
	}
	if cursor.Set && cursorTxt.Set {
		score += math.Min(contrast(cursorTxt.Color, cursor.Color)/21.0, 1.0) * 5.0
	} else {
		score += 2.5
	}
	if selFG.Set && selBG.Set {
		score += math.Min(contrast(selFG.Color, selBG.Color)/21.0, 1.0) * 10.0
	} else {
		score += 5.0
	}
	if selBG.Set {
		score += math.Min(contrast(selBG.Color, bg)/21.0, 1.0) * 5.0
	} else {
		score += 2.5
	}
	j.Score = math.Round(score*100) / 100

	return j, nil
}

func writeHTML(data []byte) {
	html := `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="description" content="WCAG 2.2 and APCA accessibility analysis of 463 Ghostty terminal color themes. Contrast ratios, blue light index, color temperature, and palette distinguishability.">
<meta property="og:title" content="Ghostty Themes — Accessibility Report">
<meta property="og:description" content="WCAG 2.2 + APCA contrast analysis for 463 Ghostty terminal themes">
<meta property="og:type" content="website">
<meta name="theme-color" content="#0e0e10">
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>👻</text></svg>">
<title>Ghostty Themes — WCAG 2.2 Accessibility Report</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{--bg:#0e0e10;--bg2:#1a1a2e;--bg3:#232340;--fg:#e0e0e8;--fg2:#a0a0b8;--accent:#7c6cf0;--accent2:#a78bfa;--green:#34d399;--yellow:#fbbf24;--orange:#fb923c;--red:#f87171;--border:#2a2a44;--radius:10px}
body{font-family:'SF Mono',SFMono-Regular,ui-monospace,'Cascadia Mono',Menlo,Consolas,monospace;background:var(--bg);color:var(--fg);line-height:1.5;min-height:100vh;-webkit-font-smoothing:antialiased}
a{color:var(--accent2);text-decoration:none}
.sr-only{position:absolute;width:1px;height:1px;padding:0;margin:-1px;overflow:hidden;clip:rect(0,0,0,0);white-space:nowrap;border:0}
:focus-visible{outline:2px solid var(--accent);outline-offset:2px;border-radius:4px}
@media(prefers-reduced-motion:reduce){*,.card,.swatch,.filter-btn,.info-btn,.modal-close,.search,.preview-cursor{animation:none!important;transition:none!important}}

header{background:var(--bg2);border-bottom:1px solid var(--border);padding:16px 32px;position:sticky;top:0;z-index:100;backdrop-filter:blur(12px)}
@media(max-width:480px){header{padding:12px}}
.header-top{display:flex;align-items:center;justify-content:space-between;gap:16px;margin-bottom:12px}
header h1{font-size:18px;font-weight:600}
.header-nav{display:flex;align-items:center;gap:10px}
.nav-link{font-size:12px;padding:6px 14px;border:1px solid var(--border);border-radius:6px;color:var(--fg2);transition:background .15s,color .15s}
.nav-link:hover{background:var(--bg3);color:var(--fg)}
.controls{display:flex;gap:12px;margin-top:16px;flex-wrap:wrap;align-items:center}
.filter-group{display:flex;gap:6px;flex-wrap:wrap}
.search{flex:1;min-width:200px;padding:10px 16px;min-height:44px;background:var(--bg);border:1px solid var(--border);border-radius:8px;color:var(--fg);font-family:inherit;font-size:14px;outline:none;transition:border-color .2s}
.search:hover{border-color:var(--fg2)}
.search:focus{border-color:var(--accent)}
.search::placeholder{color:var(--fg2)}
.filter-btn{padding:8px 16px;min-height:44px;background:var(--bg);border:1px solid var(--border);border-radius:8px;color:var(--fg2);cursor:pointer;font-family:inherit;font-size:12px;transition:background .15s,color .15s,border-color .15s}
.filter-btn:hover{background:var(--bg3);color:var(--fg);border-color:var(--fg2)}
.filter-btn:active{transform:scale(.97)}
.filter-btn[aria-pressed="true"]{background:var(--accent);color:#fff;border-color:var(--accent)}
.filter-btn[aria-pressed="true"]:hover{background:var(--accent2)}
.stats{font-size:12px;color:var(--fg2);margin-left:auto;white-space:nowrap;font-variant-numeric:tabular-nums}

.container{max-width:1400px;margin:0 auto;padding:24px}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(min(520px,100%),1fr));gap:16px}

/* List layout: single column, code left + info right */
.grid.layout-list{grid-template-columns:1fr}
.layout-list .card{display:grid;grid-template-columns:1fr 1fr;grid-template-rows:auto auto auto;contain-intrinsic-size:auto 320px}
.layout-list .card-head{grid-column:1/-1;grid-row:1}
.layout-list .preview{grid-column:1;grid-row:2/4;margin:12px 16px;align-self:stretch}
.layout-list .palette-section{grid-column:2;grid-row:2}
.layout-list .details{grid-column:2;grid-row:3;align-self:start}
@media(max-width:700px){
  .layout-list .card{grid-template-columns:1fr;grid-template-rows:auto}
  .layout-list .preview,.layout-list .palette-section,.layout-list .details{grid-column:1;grid-row:auto}
}

/* Compact layout: dense grid, no preview */
.grid.layout-compact{grid-template-columns:repeat(auto-fill,minmax(min(300px,100%),1fr));gap:10px}
.layout-compact .preview{display:none}
.layout-compact .card{contain-intrinsic-size:0 180px}
.layout-compact .card-head{padding:10px 14px}
.layout-compact .palette-section{padding:6px 14px 8px}
.layout-compact .details{padding:0 14px 10px;grid-template-columns:1fr}

.card{background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);overflow:hidden;transition:transform .15s,box-shadow .15s;content-visibility:auto;contain-intrinsic-size:0 420px}
.card:hover{transform:translateY(-2px);box-shadow:0 8px 32px rgba(0,0,0,.3)}

.card-head{display:flex;align-items:center;padding:14px 16px;border-bottom:1px solid var(--border);gap:8px}
.card-rank{font-size:11px;color:var(--fg2);background:var(--bg);padding:2px 8px;border-radius:4px;flex-shrink:0;font-variant-numeric:tabular-nums}
.card-name{font-weight:600;font-size:14px;flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.card-score{font-size:13px;font-weight:700;padding:4px 10px;border-radius:6px;flex-shrink:0;font-variant-numeric:tabular-nums;min-width:48px;text-align:center}
.score-high{background:rgba(52,211,153,.15);color:var(--green)}
.score-mid{background:rgba(251,191,36,.15);color:var(--yellow)}
.score-low{background:rgba(248,113,113,.15);color:var(--red)}

.badge{display:inline-block;font-size:10px;font-weight:700;padding:2px 6px;border-radius:4px;margin-left:6px}
.badge-aaa{background:rgba(52,211,153,.2);color:var(--green)}
.badge-aa{background:rgba(251,191,36,.2);color:var(--yellow)}
.badge-aalarge{background:rgba(251,146,60,.2);color:var(--orange)}
.badge-fail{background:rgba(248,113,113,.2);color:var(--red)}
.badge-pass{background:rgba(52,211,153,.2);color:var(--green)}

.preview{margin:12px 16px;border-radius:8px;overflow:hidden;font-size:12px;line-height:1.6;border:1px solid var(--border)}
.preview-bar{padding:6px 12px;font-size:10px;display:flex;gap:12px;opacity:.7}
.preview-body{padding:12px 16px;font-size:13px}
.preview-line{white-space:pre}
.preview-sel{border-radius:3px;padding:0 2px}
.preview-cursor{border-radius:2px;padding:0 2px;animation:blink 1.2s step-end infinite}
@keyframes blink{50%{opacity:.4}}

.palette-section{padding:8px 16px 12px}
.palette-label{font-size:10px;color:var(--fg2);margin-bottom:6px;text-transform:uppercase;letter-spacing:.5px}
.palette{display:grid;grid-template-columns:repeat(8,1fr);gap:3px}
.swatch{aspect-ratio:1;border-radius:4px;position:relative;cursor:pointer;transition:transform .1s;border:1px solid rgba(255,255,255,.06)}
.swatch:hover,.swatch:focus-visible{transform:scale(1.2);z-index:2}
.swatch:focus-visible{outline:2px solid var(--accent);outline-offset:1px}
.swatch-tip{display:none;position:absolute;bottom:calc(100% + 6px);left:50%;transform:translateX(-50%);background:var(--bg);color:var(--fg);font-size:10px;padding:5px 9px;border-radius:5px;white-space:nowrap;z-index:10;pointer-events:none;box-shadow:0 2px 12px rgba(0,0,0,.6);border:1px solid var(--border);font-variant-numeric:tabular-nums}
.swatch:hover .swatch-tip,.swatch:focus .swatch-tip{display:block}
.palette>:first-child .swatch-tip{left:0;transform:none}
.palette>:nth-child(8) .swatch-tip,.palette>:last-child .swatch-tip{left:auto;right:0;transform:none}

.details{padding:0 16px 14px;display:grid;grid-template-columns:1fr 1fr;gap:6px 16px;font-size:11px}
@media(max-width:480px){.details{grid-template-columns:1fr}}
.detail-row{display:flex;justify-content:space-between;align-items:center;gap:6px}
.detail-label{color:var(--fg2);white-space:nowrap}
.detail-val{font-weight:600;display:flex;align-items:center;gap:4px;font-variant-numeric:tabular-nums}
.cr{opacity:.7;font-weight:400}
.metric-row{display:none}
body.show-bli .metric-bli,body.show-cct .metric-cct,body.show-pde .metric-pde,body.show-phs .metric-phs,body.show-apca .metric-apca{display:flex}
.metric-toggle{padding:6px 12px;min-height:36px;background:transparent;border:1px solid var(--border);border-radius:6px;color:var(--fg2);cursor:pointer;font-family:inherit;font-size:11px;transition:background .15s,color .15s,border-color .15s}
.metric-toggle:hover{background:var(--bg3);color:var(--fg)}
.metric-toggle:active{transform:scale(.97)}
.metric-toggle[aria-pressed="true"]{background:rgba(167,139,250,.15);color:var(--accent2);border-color:var(--accent2)}
.metric-group{display:flex;gap:5px;flex-wrap:wrap;align-items:center}
.metric-group-label{font-size:10px;color:var(--fg2);margin-right:2px}
.score-toggle{border-style:dashed}
.score-toggle[aria-pressed="true"]{border-style:solid;background:rgba(52,211,153,.15);color:var(--green);border-color:var(--green)}

.empty{grid-column:1/-1;text-align:center;padding:80px 20px;color:var(--fg2);font-size:16px}

.info-btn{padding:8px 16px;min-height:44px;background:var(--bg);border:1px solid var(--border);border-radius:8px;color:var(--accent2);cursor:pointer;font-family:inherit;font-size:12px;font-weight:600;transition:background .15s,color .15s,border-color .15s}
.info-btn:hover{background:var(--bg3);color:var(--fg);border-color:var(--accent)}
.info-btn:active{transform:scale(.97)}

.modal-overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,.7);z-index:1000;justify-content:center;align-items:center;backdrop-filter:blur(4px)}
.modal-overlay.open{display:flex}
.modal{background:var(--bg2);border:1px solid var(--border);border-radius:14px;max-width:720px;width:90vw;max-height:85vh;overflow-y:auto;padding:32px;position:relative;box-shadow:0 24px 64px rgba(0,0,0,.5)}
.modal-close{position:absolute;top:16px;right:16px;background:none;border:none;color:var(--fg2);font-size:22px;cursor:pointer;width:36px;height:36px;display:flex;align-items:center;justify-content:center;border-radius:6px;transition:background .15s,color .15s}
.modal-close:hover{background:var(--bg3);color:var(--fg)}
.modal-close:active{background:var(--border)}
.modal h2{font-size:18px;margin-bottom:20px;color:var(--fg)}
.modal h3{font-size:14px;color:var(--accent2);margin:20px 0 10px;padding-top:16px;border-top:1px solid var(--border)}
.modal h3:first-of-type{border-top:none;margin-top:12px}
.modal p,.modal li{font-size:13px;color:var(--fg2);line-height:1.7}
.modal ul{padding-left:20px;margin:6px 0}
.modal li{margin:4px 0}
.modal code{background:var(--bg);padding:2px 6px;border-radius:4px;font-size:12px;color:var(--accent2)}
.modal .formula{background:var(--bg);border:1px solid var(--border);border-radius:8px;padding:14px 18px;margin:12px 0;font-size:12px;line-height:1.8;color:var(--fg);overflow-x:auto}
.modal .score-table{width:100%;border-collapse:collapse;margin:12px 0;font-size:12px}
.modal .score-table th{text-align:left;color:var(--fg);padding:8px 10px;border-bottom:2px solid var(--border);font-weight:600}
.modal .score-table td{padding:8px 10px;border-bottom:1px solid var(--border);color:var(--fg2)}
.modal .score-table td:last-child{text-align:right;font-weight:600;color:var(--fg)}
.modal .check-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin:10px 0}
@media(max-width:500px){.modal .check-grid{grid-template-columns:1fr}}
.modal .check-item{background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:10px 12px;font-size:11px;line-height:1.6}
.modal .check-item strong{color:var(--fg);font-size:12px;display:block;margin-bottom:2px}
.modal .check-item .threshold{color:var(--accent2)}
.modal a{color:var(--accent2);text-decoration:underline;text-underline-offset:2px}
</style>
</head>
<body>

<header>
  <div class="header-top">
    <h1>Ghostty Themes — WCAG 2.2 Accessibility Report</h1>
    <nav class="header-nav">
      <a href="picker.html" class="nav-link">Theme Picker</a>
      <button class="info-btn" id="infoBtn" aria-haspopup="dialog">How is this calculated?</button>
    </nav>
  </div>
  <div class="controls">
    <label for="search" class="sr-only">Search themes</label>
    <input class="search" type="search" placeholder="Search themes..." id="search" autocomplete="off">
    <div class="filter-group" role="group" aria-label="Filter themes">
      <button class="filter-btn" aria-pressed="true" data-filter="all">All</button>
      <button class="filter-btn" aria-pressed="false" data-filter="dark">Dark</button>
      <button class="filter-btn" aria-pressed="false" data-filter="light">Light</button>
    </div>
    <div class="filter-group" role="group" aria-label="Layout">
      <button class="filter-btn layout-btn" aria-pressed="true" data-layout="grid">Grid</button>
      <button class="filter-btn layout-btn" aria-pressed="false" data-layout="list">List</button>
      <button class="filter-btn layout-btn" aria-pressed="false" data-layout="compact">Compact</button>
    </div>
    <div class="metric-group" role="group" aria-label="Extra metrics">
      <span class="metric-group-label">Metrics:</span>
      <button class="metric-toggle" aria-pressed="false" data-metric="bli">Blue Light</button>
      <button class="metric-toggle" aria-pressed="false" data-metric="cct">CCT</button>
      <button class="metric-toggle" aria-pressed="false" data-metric="pde">DeltaE</button>
      <button class="metric-toggle" aria-pressed="false" data-metric="phs">Hue Coverage</button>
      <button class="metric-toggle" aria-pressed="false" data-metric="apca">APCA</button>
      <button class="metric-toggle score-toggle" aria-pressed="false" id="scoreToggle">Include in Score</button>
    </div>
    <span class="stats" id="stats" aria-live="polite"></span>
  </div>
</header>

<div class="modal-overlay" id="modal" role="dialog" aria-modal="true" aria-labelledby="modal-title">
  <div class="modal">
    <button class="modal-close" id="modalClose" aria-label="Close dialog">&times;</button>
    <h2 id="modal-title">WCAG 2.2 Accessibility Scoring</h2>
    <p>This report analyzes terminal color themes against the <a href="https://www.w3.org/TR/WCAG22/" target="_blank" rel="noopener">Web Content Accessibility Guidelines (WCAG) 2.2</a> standard, using three success criteria related to color contrast.</p>

    <h3>WCAG 2.2 Success Criteria</h3>
    <div class="check-grid">
      <div class="check-item">
        <strong>SC 1.4.3 &mdash; Contrast Minimum (Level AA)</strong>
        Normal text: <span class="threshold">&ge; 4.5 : 1</span><br>
        Large text: <span class="threshold">&ge; 3.0 : 1</span>
      </div>
      <div class="check-item">
        <strong>SC 1.4.6 &mdash; Contrast Enhanced (Level AAA)</strong>
        Normal text: <span class="threshold">&ge; 7.0 : 1</span><br>
        Large text: <span class="threshold">&ge; 4.5 : 1</span>
      </div>
      <div class="check-item">
        <strong>SC 1.4.11 &mdash; Non-text Contrast (Level AA)</strong>
        UI components: <span class="threshold">&ge; 3.0 : 1</span><br>
        Applied to cursor &amp; selection regions
      </div>
      <div class="check-item">
        <strong>Conformance Levels</strong>
        <span class="threshold">AAA</span> = highest (enhanced)<br>
        <span class="threshold">AA</span> = minimum recommended<br>
        <span class="threshold">AA-Large</span> = large text only
      </div>
    </div>

    <h3>Contrast Ratio Formula</h3>
    <p>Per WCAG 2.2 and the <a href="https://www.w3.org/WAI/WCAG22/Techniques/general/G17" target="_blank" rel="noopener">G17 technique</a>:</p>
    <div class="formula">
      <strong>1. Normalize sRGB</strong> (8-bit to 0&ndash;1):<br>
      &nbsp;&nbsp;R<sub>sRGB</sub> = R<sub>8bit</sub> / 255<br><br>
      <strong>2. Linearize each channel:</strong><br>
      &nbsp;&nbsp;if R<sub>sRGB</sub> &le; 0.04045 &rarr; R = R<sub>sRGB</sub> / 12.92<br>
      &nbsp;&nbsp;else &rarr; R = ((R<sub>sRGB</sub> + 0.055) / 1.055)<sup>2.4</sup><br><br>
      <strong>3. Relative luminance:</strong><br>
      &nbsp;&nbsp;L = 0.2126 &times; R + 0.7152 &times; G + 0.0722 &times; B<br><br>
      <strong>4. Contrast ratio</strong> (L1 = lighter, L2 = darker):<br>
      &nbsp;&nbsp;CR = (L1 + 0.05) / (L2 + 0.05)<br>
      &nbsp;&nbsp;Range: 1 : 1 (identical) to 21 : 1 (black on white)
    </div>

    <h3>What Colors Are Checked</h3>
    <p>Every theme defines up to 22 colors. We check 6 contrast pairs:</p>
    <table class="score-table">
      <thead><tr><th>Pair</th><th>WCAG Criterion</th><th>What it Tests</th></tr></thead>
      <tbody>
      <tr><td><code>foreground</code> vs <code>background</code></td><td>SC 1.4.3 / 1.4.6</td><td>Main text readability</td></tr>
      <tr><td>16 <code>palette</code> colors vs <code>background</code></td><td>SC 1.4.3 / 1.4.6</td><td>ANSI color readability</td></tr>
      <tr><td><code>cursor-color</code> vs <code>background</code></td><td>SC 1.4.11</td><td>Cursor visibility</td></tr>
      <tr><td><code>cursor-text</code> vs <code>cursor-color</code></td><td>SC 1.4.3 / 1.4.6</td><td>Text under cursor</td></tr>
      <tr><td><code>selection-foreground</code> vs <code>selection-background</code></td><td>SC 1.4.3 / 1.4.6</td><td>Selected text readability</td></tr>
      <tr><td><code>selection-background</code> vs <code>background</code></td><td>SC 1.4.11</td><td>Selection distinguishability</td></tr>
      </tbody>
    </table>

    <h3>Score Calculation (0&ndash;100)</h3>
    <p>Each component maps its contrast ratio to points. Ratio-based scores use <code>min(ratio / 21, 1) &times; weight</code>. Palette scores use the fraction of 16 colors passing the threshold.</p>
    <table class="score-table">
      <thead><tr><th>Component</th><th>Formula</th><th>Max</th></tr></thead>
      <tbody>
      <tr><td>FG / BG contrast</td><td><code>min(ratio / 21, 1) &times; 30</code></td><td>30 pts</td></tr>
      <tr><td>Palette AA passes (&ge; 4.5:1)</td><td><code>(count / 16) &times; 25</code></td><td>25 pts</td></tr>
      <tr><td>Palette AAA passes (&ge; 7:1)</td><td><code>(count / 16) &times; 15</code></td><td>15 pts</td></tr>
      <tr><td>Cursor vs BG</td><td><code>min(ratio / 21, 1) &times; 10</code></td><td>10 pts</td></tr>
      <tr><td>Cursor text vs cursor</td><td><code>min(ratio / 21, 1) &times; 5</code></td><td>5 pts</td></tr>
      <tr><td>Selection text</td><td><code>min(ratio / 21, 1) &times; 10</code></td><td>10 pts</td></tr>
      <tr><td>Selection visibility</td><td><code>min(ratio / 21, 1) &times; 5</code></td><td>5 pts</td></tr>
      </tbody>
      <tfoot><tr><td></td><td></td><td><strong>100 pts</strong></td></tr></tfoot>
    </table>
    <p>When a color is absent (e.g., no <code>cursor-color</code> defined), half the possible points are awarded as a neutral default.</p>

    <h3>APCA — WCAG 3.0 Contrast (toggleable)</h3>
    <p>The <a href="https://github.com/Myndex/apca-w3" target="_blank" rel="noopener">APCA</a> (Accessible Perceptual Contrast Algorithm) is the proposed replacement for WCAG 2.x contrast ratio in WCAG 3.0. Key differences:</p>
    <ul>
      <li><strong>Polarity-aware</strong>: light-on-dark and dark-on-light are scored differently (human vision is asymmetric)</li>
      <li><strong>Better perceptual model</strong>: uses tuned exponents instead of a simple luminance ratio</li>
      <li><strong>Font-size-aware</strong>: thresholds vary by text size and weight</li>
    </ul>
    <div class="formula">
      <strong>Luminance:</strong> Y = 0.2126729&middot;(R/255)<sup>2.4</sup> + 0.7151522&middot;(G/255)<sup>2.4</sup> + 0.0721750&middot;(B/255)<sup>2.4</sup><br>
      <strong>Soft clamp:</strong> if Y &lt; 0.022 &rarr; Y += (0.022 &minus; Y)<sup>1.414</sup><br>
      <strong>Normal</strong> (dark on light): Lc = ((bgY<sup>0.56</sup> &minus; txtY<sup>0.57</sup>) &times; 1.14 &minus; 0.027) &times; 100<br>
      <strong>Reverse</strong> (light on dark): Lc = ((bgY<sup>0.65</sup> &minus; txtY<sup>0.62</sup>) &times; 1.14 + 0.027) &times; 100
    </div>
    <p>Thresholds (at W400 normal weight): <strong>|Lc| &ge; 75</strong> for body text (~18px), <strong>|Lc| &ge; 60</strong> for large text (~24px), <strong>|Lc| &ge; 45</strong> for headlines only.</p>

    <h3>Extra Metrics (toggleable)</h3>
    <table class="score-table">
      <thead><tr><th>Metric</th><th>What it Measures</th><th>Formula</th></tr></thead>
      <tbody>
      <tr><td>Blue Light Index</td><td>Blue emission from background (0&ndash;1). Lower = warmer, easier on eyes at night.</td><td><code>B<sub>lin</sub> / (R<sub>lin</sub> + G<sub>lin</sub> + B<sub>lin</sub>)</code></td></tr>
      <tr><td>CCT (K)</td><td>Correlated Color Temperature. &lt; 4000 K = warm, &gt; 6500 K = cool/harsh.</td><td>McCamy&rsquo;s approximation from CIE xy chromaticity</td></tr>
      <tr><td>Min DeltaE</td><td>Closest pair of palette colors. Low = two colors look the same. &gt; 10 is good.</td><td>CIE76: <code>&radic;(&Delta;L&sup2; + &Delta;a&sup2; + &Delta;b&sup2;)</code> in CIELAB</td></tr>
      <tr><td>Hue Coverage</td><td>How spread out the palette hues are. Higher = richer variety. &gt; 60&deg; is good.</td><td>Circular std. dev. of <code>atan2(b*, a*)</code> in CIELAB</td></tr>
      </tbody>
    </table>
    <p>These metrics are hidden by default. Toggle each to see values per card. Click <strong>Include in Score</strong> to blend them into the ranking:</p>
    <table class="score-table">
      <thead><tr><th>Component</th><th>Enhanced Formula</th><th>Max</th></tr></thead>
      <tbody>
      <tr><td>WCAG score</td><td><code>original &times; 0.7</code></td><td>70 pts</td></tr>
      <tr><td>Blue Light</td><td><code>(1 &minus; index) &times; 7.5</code></td><td>7.5 pts</td></tr>
      <tr><td>CCT warmth</td><td><code>(10000 &minus; K) / 8000 &times; 7.5</code></td><td>7.5 pts</td></tr>
      <tr><td>Min DeltaE</td><td><code>min(deltaE / 30, 1) &times; 7.5</code></td><td>7.5 pts</td></tr>
      <tr><td>Hue Coverage</td><td><code>min(stddev / 120, 1) &times; 7.5</code></td><td>7.5 pts</td></tr>
      </tbody>
      <tfoot><tr><td></td><td></td><td><strong>100 pts</strong></td></tr></tfoot>
    </table>

    <h3>Filters &amp; Layouts</h3>
    <ul>
      <li><strong>Dark / Light</strong> &mdash; Filter by background luminance (dark &lt; 0.5).</li>
      <li><strong>Grid</strong> &mdash; Default card grid, good for browsing.</li>
      <li><strong>List</strong> &mdash; Single-column, code preview side by side with details.</li>
      <li><strong>Compact</strong> &mdash; Dense grid, hides code preview for scanning.</li>
    </ul>
  </div>
</div>

<main class="container">
  <div class="grid" id="grid" role="list" aria-label="Theme cards"></div>
</main>

<script>
const themes=JSON.parse(` + "`" + string(data) + "`" + `);

// Enhanced score: WCAG 70pts + blue light 7.5 + CCT 7.5 + deltaE 7.5 + hue 7.5
function enhancedScore(t){
  // WCAG portion: scale original 0-100 score to 0-70
  let es=t.s*0.7;
  // Blue light: lower = better (0.0 best, 1.0 worst) → (1-bli)*7.5
  es+=(1-Math.min(t.bli,1))*7.5;
  // CCT warmth: 2000K=best, 10000K=worst. Map 2000-10000 → 1-0. Black (0K) = full points.
  if(t.cct>0){es+=Math.max(0,Math.min(1,(10000-t.cct)/8000))*7.5}else{es+=7.5}
  // Min deltaE: higher=better, cap at 30
  es+=Math.min(t.pde/30,1)*7.5;
  // Hue coverage: higher=better, cap at 120°
  es+=Math.min(t.phs/120,1)*7.5;
  return Math.round(es*100)/100;
}

let useEnhanced=false;
let sortedIndices=null;

// Pre-compute: isDark flag, lowercase name, enhanced score, sort indices
for(let i=0;i<themes.length;i++){
  const t=themes[i];
  const r=parseInt(t.b.slice(1,3),16),g=parseInt(t.b.slice(3,5),16),b=parseInt(t.b.slice(5,7),16);
  t._dark=(0.2126*r+0.7152*g+0.0722*b)/255<0.5;
  t._lname=t.n.toLowerCase();
  t._es=enhancedScore(t);
}

function getSortedIndices(){
  if(sortedIndices)return sortedIndices;
  const idxs=Array.from({length:themes.length},(_,i)=>i);
  idxs.sort((a,b)=>{
    const sa=useEnhanced?themes[a]._es:themes[a].s;
    const sb=useEnhanced?themes[b]._es:themes[b].s;
    if(sa!==sb)return sb-sa;
    return themes[a].n<themes[b].n?-1:1;
  });
  sortedIndices=idxs;
  return idxs;
}

function getScore(t){return Math.round(useEnhanced?t._es:t.s)}

function scoreClass(s){return s>=60?'score-high':s>=35?'score-mid':'score-low'}

function badgeHTML(lv,nt){
  if(nt)return '<span class="badge badge-'+(lv==='Pass'?'pass':'fail')+'">'+lv+'</span>';
  const c=lv==='AAA'?'aaa':lv==='AA'?'aa':lv==='AA-Large'?'aalarge':'fail';
  return '<span class="badge badge-'+c+'">'+lv+'</span>';
}

function metricRow(cls,label,val,tag){
  return '<div class="detail-row metric-row '+cls+'"><span class="detail-label">'+label+'</span><span class="detail-val">'+val+' '+tag+'</span></div>';
}
function bliLabel(v){return v<0.25?'<span class="badge badge-aaa">Warm</span>':v<0.35?'<span class="badge badge-aa">Neutral</span>':'<span class="badge badge-fail">Cool</span>'}
function cctLabel(v){if(v<=0)return'<span class="badge badge-aaa">No emission</span>';return v<4000?'<span class="badge badge-aaa">Warm</span>':v<6500?'<span class="badge badge-aa">Neutral</span>':'<span class="badge badge-fail">Cool</span>'}
function pdeLabel(v){return v>=10?'<span class="badge badge-aaa">Good</span>':v>=5?'<span class="badge badge-aa">Fair</span>':'<span class="badge badge-fail">Poor</span>'}
function phsLabel(v){return v>=60?'<span class="badge badge-aaa">Rich</span>':v>=40?'<span class="badge badge-aa">Moderate</span>':'<span class="badge badge-fail">Narrow</span>'}
function apcaLabel(lc){const a=Math.abs(lc);return a>=75?'<span class="badge badge-aaa">Body text</span>':a>=60?'<span class="badge badge-aa">Large text</span>':a>=45?'<span class="badge badge-aalarge">Headlines</span>':'<span class="badge badge-fail">Fail</span>'}
function apcaPalLabel(n){return n>=14?'<span class="badge badge-aaa">Good</span>':n>=10?'<span class="badge badge-aa">Fair</span>':'<span class="badge badge-fail">Poor</span>'}

function pairRow(label,info){
  if(!info)return '<div class="detail-row"><span class="detail-label">'+label+'</span><span class="detail-val" style="opacity:.4">N/A</span></div>';
  return '<div class="detail-row"><span class="detail-label">'+label+'</span><span class="detail-val"><span class="cr">'+info.r.toFixed(2)+':1</span>'+badgeHTML(info.l,info.nt||false)+'</span></div>';
}

// Card body cache: static parts (preview, palette, details) rendered once.
// Header (rank, score) is dynamic since score mode and sort order can change.
const bodyCache=new Array(themes.length);
function getCard(idx,displayRank){
  const t=themes[idx];
  const sc=getScore(t);
  let body=bodyCache[idx];
  if(!body){
    const dk=t._dark,P=t.p;
  const kw=P[4].h,st=P[2].h,nm=P[1].h,cm=P[8].h,fn=P[6].h,pa=P[7].h;
  const sB=t.sl?t.sl.y:dk?'rgba(255,255,255,.15)':'rgba(0,0,0,.12)';
  const sF=t.sl?t.sl.x:t.f;
  const cB=t.c?t.c.x:t.f;
  const cF=t.ct?t.ct.x:t.b;
  const syn=[
    '<span style="color:'+kw+'">func</span> <span style="color:'+fn+'">main</span><span style="color:'+pa+'">()</span> <span style="color:'+pa+'">{</span>',
    '    <span style="color:'+kw+'">msg</span> := <span style="color:'+st+'">"Hello, World!"</span>',
    '    <span style="color:'+kw+'">count</span> := <span style="color:'+nm+'">42</span>',
    '    <span style="color:'+fn+'">fmt</span>.<span style="color:'+fn+'">Println</span>(<span style="color:'+kw+'">msg</span>, <span style="color:'+kw+'">count</span>)',
    '    <span style="color:'+cm+'">// WCAG 2.2 check</span>',
    '<span style="color:'+pa+'">}</span>',
  ];
  let pv='<div class="preview-bar" style="background:'+t.b+';color:'+t.f+';border-bottom:1px solid '+(dk?'rgba(255,255,255,.08)':'rgba(0,0,0,.08)')+'"><span>'+t.n+'</span><span style="margin-left:auto">~/code</span></div>';
  pv+='<div class="preview-body" style="background:'+t.b+';color:'+t.f+'">';
  for(let j=0;j<syn.length;j++){
    let ln='<span style="color:'+cm+';opacity:.5">'+(j+1)+'</span>  '+syn[j];
    if(j===1)ln='<span class="preview-sel" style="background:'+sB+';color:'+sF+'">'+ln+'</span>';
    if(j===3)ln='<span style="color:'+cm+';opacity:.5">4</span>  '+syn[j].slice(0,-1)+'<span class="preview-cursor" style="background:'+cB+';color:'+cF+'">)</span>';
    pv+='<div class="preview-line">'+ln+'</div>';
  }
  pv+='</div>';
  let sw='';
  for(let j=0;j<16;j++){
    const c=P[j];
    sw+='<div class="swatch" tabindex="0" role="img" aria-label="Color '+j+': '+c.h+', contrast '+c.r.toFixed(1)+':1, '+c.l+'" style="background:'+c.h+'"><div class="swatch-tip">'+j+': '+c.h+'<br>'+c.r.toFixed(1)+':1 '+c.l+'</div></div>';
  }
  body='<div class="preview">'+pv+'</div>'+
    '<div class="palette-section"><div class="palette-label">Palette — '+t.a+'/16 AA, '+t.A+'/16 AAA vs background</div><div class="palette">'+sw+'</div></div>'+
    '<div class="details">'+
      '<div class="detail-row"><span class="detail-label">FG / BG</span><span class="detail-val"><span class="cr">'+t.fc.toFixed(2)+':1</span>'+badgeHTML(t.fl)+'</span></div>'+
      pairRow('Cursor (1.4.11)',t.c)+pairRow('Cursor text',t.ct)+pairRow('Selection text',t.sl)+pairRow('Selection vis. (1.4.11)',t.sd)+
      '<div class="detail-row"><span class="detail-label">Palette AA / AAA</span><span class="detail-val">'+t.a+' / '+t.A+' of 16</span></div>'+
      metricRow('metric-bli','Blue Light',t.bli.toFixed(3),bliLabel(t.bli))+
      metricRow('metric-cct','CCT',t.cct>0?Math.round(t.cct)+' K':'0 K (black)',cctLabel(t.cct))+
      metricRow('metric-pde','Min DeltaE',t.pde.toFixed(1),pdeLabel(t.pde))+
      metricRow('metric-phs','Hue Coverage',t.phs.toFixed(1)+'°',phsLabel(t.phs))+
      metricRow('metric-apca','APCA FG/BG','Lc '+t.alc.toFixed(1),apcaLabel(t.alc))+
      metricRow('metric-apca','APCA Palette',t.ap75+'/16 body, '+t.ap60+'/16 large',apcaPalLabel(t.ap75))+
    '</div>';
    bodyCache[idx]=body;
  }
  // Dynamic header: rank and score change with score mode
  return '<div class="card" role="listitem">'+
    '<div class="card-head"><span class="card-rank">#'+displayRank+'</span><span class="card-name">'+t.n+'</span><span class="card-score '+scoreClass(sc)+'">'+sc+'</span></div>'+
    body+'</div>';
}

const grid=document.getElementById('grid');
const search=document.getElementById('search');
const stats=document.getElementById('stats');
let activeFilter='all';

function render(){
  const q=search.value.toLowerCase();
  const sorted=getSortedIndices();
  const parts=[];
  let filtered=[];
  for(const i of sorted){
    const t=themes[i];
    if(q&&!t._lname.includes(q))continue;
    if(activeFilter==='dark'&&!t._dark)continue;
    if(activeFilter==='light'&&t._dark)continue;
    filtered.push(i);
  }

  // Dense ranking: same score = same rank
  let rank=1,prevScore=-1;
  for(let r=0;r<filtered.length;r++){
    const sc=getScore(themes[filtered[r]]);
    if(sc!==prevScore){rank=r+1;prevScore=sc}
    parts.push(getCard(filtered[r],rank));
  }
  if(!parts.length){
    const hint=q?'Try a different search term or clear the search field.':'Try a different filter.';
    parts.push('<div class="empty">No themes match your criteria.<br><span style="font-size:13px;opacity:.7">'+hint+'</span></div>');
  }

  requestAnimationFrame(()=>{
    grid.innerHTML=parts.join('');
  });
  stats.textContent=filtered.length+' of '+themes.length+' themes';
}

// Debounced search (150ms)
let searchTimer;
search.addEventListener('input',()=>{
  clearTimeout(searchTimer);
  searchTimer=setTimeout(render,150);
});

// Filter buttons (All / Dark / Light)
document.querySelectorAll('.filter-btn:not(.layout-btn)').forEach(btn=>{
  btn.addEventListener('click',()=>{
    document.querySelectorAll('.filter-btn:not(.layout-btn)').forEach(b=>b.setAttribute('aria-pressed','false'));
    btn.setAttribute('aria-pressed','true');
    activeFilter=btn.dataset.filter;
    render();
  });
});

// Layout buttons (Grid / List / Compact)
document.querySelectorAll('.layout-btn').forEach(btn=>{
  btn.addEventListener('click',()=>{
    document.querySelectorAll('.layout-btn').forEach(b=>b.setAttribute('aria-pressed','false'));
    btn.setAttribute('aria-pressed','true');
    grid.className='grid layout-'+btn.dataset.layout;
  });
});

// Metric toggles (independent, CSS-driven, no re-render)
document.querySelectorAll('.metric-toggle:not(.score-toggle)').forEach(btn=>{
  btn.addEventListener('click',()=>{
    const m=btn.dataset.metric;
    const on=btn.getAttribute('aria-pressed')==='true';
    btn.setAttribute('aria-pressed',on?'false':'true');
    document.body.classList.toggle('show-'+m);
  });
});

// Score toggle: include extra metrics in score, re-sort, re-render
document.getElementById('scoreToggle').addEventListener('click',function(){
  const on=this.getAttribute('aria-pressed')==='true';
  useEnhanced=!on;
  this.setAttribute('aria-pressed',useEnhanced?'true':'false');
  sortedIndices=null; // invalidate sort order
  // Also show all 4 metrics when score includes them
  if(useEnhanced){
    ['bli','cct','pde','phs'].forEach(m=>{
      document.body.classList.add('show-'+m);
      const b=document.querySelector('[data-metric="'+m+'"]');
      if(b)b.setAttribute('aria-pressed','true');
    });
  }
  render();
});

render();

// Modal with focus trap and focus restore
const modal=document.getElementById('modal');
const infoBtn=document.getElementById('infoBtn');
const modalClose=document.getElementById('modalClose');
let lastFocused=null;

function openModal(){
  lastFocused=document.activeElement;
  modal.classList.add('open');
  modalClose.focus();
}
function closeModal(){
  modal.classList.remove('open');
  if(lastFocused)lastFocused.focus();
}

infoBtn.addEventListener('click',openModal);
modalClose.addEventListener('click',closeModal);
modal.addEventListener('click',(e)=>{if(e.target===modal)closeModal()});
document.addEventListener('keydown',(e)=>{
  if(e.key==='Escape'&&modal.classList.contains('open')){closeModal();return}
  // Focus trap when modal is open
  if(e.key==='Tab'&&modal.classList.contains('open')){
    const focusable=modal.querySelectorAll('button,a[href],input,[tabindex]:not([tabindex="-1"])');
    const first=focusable[0],last=focusable[focusable.length-1];
    if(e.shiftKey&&document.activeElement===first){e.preventDefault();last.focus()}
    else if(!e.shiftKey&&document.activeElement===last){e.preventDefault();first.focus()}
  }
});
</script>
</body>
</html>`

	outPath := filepath.Join(outDir, "index.html")
	os.WriteFile(outPath, []byte(html), 0644)
	fmt.Fprintf(os.Stderr, "Report: %s (%d bytes)\n", outPath, len(html))
}

func writePicker(data []byte) {
	html := `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="description" content="Tournament-style theme picker for Ghostty terminal. Compare themes head-to-head until you find your favorite.">
<meta property="og:title" content="Ghostty Theme Picker">
<meta property="og:description" content="Find your favorite Ghostty terminal theme in a tournament bracket">
<meta property="og:type" content="website">
<meta name="theme-color" content="#0e0e10">
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>👻</text></svg>">
<title>Ghostty Theme Picker — Tournament</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{--bg:#0e0e10;--fg:#e0e0e8;--fg2:#a0a0b8;--accent:#7c6cf0;--accent2:#a78bfa;--border:#2a2a44;--green:#34d399;--yellow:#fbbf24;--red:#f87171}
body{font-family:'SF Mono',SFMono-Regular,ui-monospace,'Cascadia Mono',Menlo,Consolas,monospace;background:var(--bg);color:var(--fg);min-height:100vh;display:flex;flex-direction:column;overflow:hidden;-webkit-font-smoothing:antialiased}
.sr-only{position:absolute;width:1px;height:1px;padding:0;margin:-1px;overflow:hidden;clip:rect(0,0,0,0);white-space:nowrap;border:0}
:focus-visible{outline:2px solid var(--accent);outline-offset:2px}
@media(prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important}}

.top-bar{display:flex;align-items:center;justify-content:space-between;padding:12px 24px;border-bottom:1px solid var(--border);flex-shrink:0;gap:16px;flex-wrap:wrap}
.top-bar h1{font-size:15px;font-weight:600;white-space:nowrap}
.progress-wrap{flex:1;max-width:400px;min-width:120px}
.progress-label{font-size:10px;color:var(--fg2);margin-bottom:4px;display:flex;justify-content:space-between}
.progress-track{height:6px;background:var(--border);border-radius:3px;overflow:hidden}
.progress-fill{height:100%;background:var(--accent);border-radius:3px;transition:width .3s ease-out}
.round-info{font-size:12px;color:var(--fg2);text-align:right;white-space:nowrap;font-variant-numeric:tabular-nums}

.arena{display:flex;flex:1;overflow:hidden;position:relative}

.panel{flex:1;display:flex;flex-direction:column;cursor:pointer;position:relative;transition:opacity .15s}
.panel:hover .panel-overlay{opacity:1}
.panel:active .panel-overlay{opacity:1;background:rgba(124,108,240,.15)}
.panel-left{border-right:1px solid var(--border)}

.panel-header{padding:14px 20px;display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid rgba(255,255,255,.06);flex-shrink:0}
.panel-name{font-size:16px;font-weight:700}
.panel-score{font-size:12px;padding:3px 8px;border-radius:4px;font-variant-numeric:tabular-nums}
.panel-score.high{background:rgba(52,211,153,.15);color:var(--green)}
.panel-score.mid{background:rgba(251,191,36,.15);color:var(--yellow)}
.panel-score.low{background:rgba(248,113,113,.15);color:var(--red)}

.panel-code{flex:1;padding:20px 24px;font-size:14px;line-height:1.7;overflow:auto}
@media(max-width:600px){.panel-code{font-size:12px;padding:14px 12px}}
.panel-code .ln{opacity:.4;display:inline-block;width:2.5ch;text-align:right;margin-right:1.5ch;user-select:none}
.panel-code .line{white-space:pre}
.panel-code .sel{border-radius:3px;padding:1px 3px}
.panel-code .cur{border-radius:2px;padding:1px 3px;animation:blink 1.2s step-end infinite}
@keyframes blink{50%{opacity:.4}}

.panel-meta{padding:10px 20px 14px;border-top:1px solid rgba(255,255,255,.06);flex-shrink:0;display:flex;gap:16px;flex-wrap:wrap;font-size:11px}
.meta-item{color:inherit;opacity:.7}
.meta-val{font-weight:600;opacity:1}

.panel-palette{display:flex;gap:3px;align-items:center}
.mini-swatch{width:14px;height:14px;border-radius:3px;border:1px solid rgba(255,255,255,.08)}

.panel-overlay{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;opacity:0;transition:opacity .15s;pointer-events:none;z-index:5}
.panel-overlay span{background:rgba(0,0,0,.7);color:#fff;padding:10px 28px;border-radius:8px;font-size:15px;font-weight:700;letter-spacing:.5px;backdrop-filter:blur(4px)}

.divider{position:absolute;left:50%;top:0;bottom:0;width:1px;background:var(--border);z-index:10}
.vs-badge{position:absolute;left:50%;top:50%;transform:translate(-50%,-50%);z-index:11;background:var(--bg);border:2px solid var(--border);border-radius:50%;width:44px;height:44px;display:flex;align-items:center;justify-content:center;font-size:13px;font-weight:700;color:var(--fg2)}

.winner-screen{display:none;flex-direction:column;align-items:center;justify-content:center;flex:1;text-align:center;padding:40px;gap:20px}
.winner-screen.show{display:flex}
.winner-screen h2{font-size:14px;color:var(--fg2);text-transform:uppercase;letter-spacing:2px}
.winner-name{font-size:32px;font-weight:700;color:var(--accent2)}
@media(max-width:600px){.winner-name{font-size:22px}}
.winner-preview{width:min(560px,90vw);border-radius:12px;overflow:hidden;border:2px solid var(--accent);box-shadow:0 0 40px rgba(124,108,240,.2)}
.winner-preview .panel-code{min-height:200px}
.winner-stats{display:flex;gap:24px;font-size:13px;color:var(--fg2);flex-wrap:wrap;justify-content:center}
.winner-stats strong{color:var(--fg)}
.btn{padding:12px 32px;border:1px solid var(--border);border-radius:8px;background:var(--accent);color:#fff;font-family:inherit;font-size:14px;font-weight:600;cursor:pointer;transition:background .15s}
.btn:hover{background:var(--accent2)}
.btn:active{transform:scale(.97)}
.btn-outline{background:transparent;color:var(--fg2);border-color:var(--border)}
.btn-outline:hover{background:rgba(255,255,255,.05);color:var(--fg)}
.winner-actions{display:flex;gap:12px;flex-wrap:wrap;justify-content:center}

.start-screen{display:flex;flex-direction:column;align-items:center;justify-content:center;flex:1;text-align:center;padding:40px;gap:24px}
.start-screen h2{font-size:22px;font-weight:700}
.start-screen p{font-size:14px;color:var(--fg2);max-width:480px;line-height:1.7}
.start-options{display:flex;gap:12px;flex-wrap:wrap;justify-content:center}
.start-count{font-size:12px;color:var(--fg2);font-variant-numeric:tabular-nums}

@media(max-width:600px){
  .arena{flex-direction:column}
  .panel-left{border-right:none;border-bottom:1px solid var(--border)}
  .divider{left:0;right:0;top:50%;bottom:auto;width:100%;height:1px}
  .vs-badge{top:50%}
  .top-bar{padding:10px 12px}
}
</style>
</head>
<body>

<div class="top-bar">
  <h1>Theme Picker</h1>
  <a href="index.html" style="font-size:12px;padding:6px 14px;border:1px solid var(--border);border-radius:6px;color:var(--fg2);text-decoration:none">Report</a>
  <div class="progress-wrap" id="progressWrap" style="display:none">
    <div class="progress-label"><span id="roundLabel"></span><span id="matchLabel"></span></div>
    <div class="progress-track"><div class="progress-fill" id="progressFill"></div></div>
  </div>
  <div class="round-info" id="roundInfo"></div>
</div>

<div class="start-screen" id="startScreen">
  <h2>Find Your Favorite Theme</h2>
  <p>Themes face off head-to-head in a tournament bracket. Pick the one you prefer in each matchup. Winners advance until your champion remains.</p>
  <div class="start-options">
    <button class="btn" onclick="startTournament(0)">All Themes</button>
    <button class="btn btn-outline" onclick="startTournament(64)">Top 64</button>
    <button class="btn btn-outline" onclick="startTournament(32)">Top 32</button>
    <button class="btn btn-outline" onclick="startTournament(16)">Top 16</button>
  </div>
  <div class="start-count" id="startCount"></div>
</div>

<div class="arena" id="arena" style="display:none">
  <div class="panel panel-left" id="panelL" tabindex="0" role="button" aria-label="Pick left theme">
    <div class="panel-header"><span class="panel-name" id="nameL"></span><span class="panel-score" id="scoreL"></span></div>
    <div class="panel-code" id="codeL"></div>
    <div class="panel-meta" id="metaL"></div>
    <div class="panel-overlay"><span>Pick this</span></div>
  </div>
  <div class="divider"></div>
  <div class="vs-badge">VS</div>
  <div class="panel" id="panelR" tabindex="0" role="button" aria-label="Pick right theme">
    <div class="panel-header"><span class="panel-name" id="nameR"></span><span class="panel-score" id="scoreR"></span></div>
    <div class="panel-code" id="codeR"></div>
    <div class="panel-meta" id="metaR"></div>
    <div class="panel-overlay"><span>Pick this</span></div>
  </div>
</div>

<div class="winner-screen" id="winnerScreen">
  <h2>Your Champion</h2>
  <div class="winner-name" id="winnerName"></div>
  <div class="winner-preview" id="winnerPreview">
    <div class="panel-code" id="winnerCode" style="min-height:220px"></div>
  </div>
  <div class="winner-stats" id="winnerStats"></div>
  <div class="winner-actions">
    <button class="btn" onclick="location.reload()">Play Again</button>
    <button class="btn btn-outline" onclick="copyThemeName()">Copy Theme Name</button>
  </div>
</div>

<script>
const ALL=JSON.parse(` + "`" + string(data) + "`" + `);

document.getElementById('startCount').textContent=ALL.length+' themes available (sorted by WCAG 2.2 accessibility score)';

// Shuffle using Fisher-Yates
function shuffle(arr){
  const a=[...arr];
  for(let i=a.length-1;i>0;i--){const j=Math.random()*i+1|0;[a[i],a[j]]=[a[j],a[i]]}
  return a;
}

let pool=[],round=0,matchIdx=0,totalMatches=0,matchesPlayed=0,winners=[];
let currentL=null,currentR=null,champion=null;

function startTournament(topN){
  let themes=topN>0?ALL.slice(0,topN):ALL.slice();
  pool=shuffle(themes);
  round=1;matchIdx=0;matchesPlayed=0;winners=[];champion=null;
  totalMatches=calcTotalMatches(pool.length);
  document.getElementById('startScreen').style.display='none';
  document.getElementById('arena').style.display='flex';
  document.getElementById('progressWrap').style.display='block';
  document.getElementById('winnerScreen').classList.remove('show');
  nextMatch();
}

function calcTotalMatches(n){
  let total=0;
  while(n>1){const m=Math.floor(n/2);total+=m;n=m+(n%2);}
  return total;
}

function nextMatch(){
  if(matchIdx+1>=pool.length){
    // This round's pool exhausted
    if(pool.length%2===1)winners.push(pool[pool.length-1]); // bye
    if(winners.length===1){showWinner(winners[0]);return}
    pool=shuffle(winners);winners=[];round++;matchIdx=0;
  }
  currentL=pool[matchIdx];currentR=pool[matchIdx+1];
  matchIdx+=2;
  renderMatch();
}

function renderMatch(){
  const roundSize=Math.ceil(pool.length/2);
  const matchInRound=Math.ceil(matchIdx/2);
  document.getElementById('roundLabel').textContent='Round '+round;
  document.getElementById('matchLabel').textContent='Match '+matchInRound+' / '+roundSize;
  document.getElementById('roundInfo').textContent=pool.length+' remaining';
  const pct=Math.round(matchesPlayed/totalMatches*100);
  document.getElementById('progressFill').style.width=pct+'%';

  renderPanel('L',currentL);
  renderPanel('R',currentR);
}

function renderPanel(side,t){
  const dk=isDark(t.b);
  const panel=document.getElementById('panel'+side);
  const name=document.getElementById('name'+side);
  const score=document.getElementById('score'+side);
  const code=document.getElementById('code'+side);
  const meta=document.getElementById('meta'+side);

  panel.style.background=t.b;panel.style.color=t.f;
  name.textContent=t.n;name.style.color=t.f;
  const sc=Math.round(t.s);
  score.textContent=sc;
  score.className='panel-score '+(sc>=60?'high':sc>=35?'mid':'low');

  const P=t.p;
  const kw=P[4].h,st=P[2].h,nm=P[1].h,cm=P[8].h,fn=P[6].h,pa=P[7].h;
  const sB=t.sl?t.sl.y:dk?'rgba(255,255,255,.12)':'rgba(0,0,0,.1)';
  const sF=t.sl?t.sl.x:t.f;
  const cB=t.c?t.c.x:t.f;
  const cF=t.ct?t.ct.x:t.b;

  const lines=[
    ln(1,cm,'<span style="color:'+kw+'">package</span> <span style="color:'+fn+'">main</span>'),
    ln(2,cm,''),
    ln(3,cm,'<span style="color:'+kw+'">import</span> <span style="color:'+st+'">"fmt"</span>'),
    ln(4,cm,''),
    sel(5,cm,sB,sF,'<span style="color:'+kw+'">func</span> <span style="color:'+fn+'">main</span><span style="color:'+pa+'">()</span> <span style="color:'+pa+'">{</span>'),
    sel(6,cm,sB,sF,'    <span style="color:'+kw+'">name</span> := <span style="color:'+st+'">"Ghostty"</span>'),
    ln(7,cm,'    <span style="color:'+kw+'">version</span> := <span style="color:'+nm+'">1.1</span>'),
    ln(8,cm,'    <span style="color:'+fn+'">fmt</span>.<span style="color:'+fn+'">Printf</span>(<span style="color:'+st+'">"%s v%.1f\\n"</span>, <span style="color:'+kw+'">name</span>, <span style="color:'+kw+'">version</span><span class="cur" style="background:'+cB+';color:'+cF+'">)</span>'),
    ln(9,cm,''),
    ln(10,cm,'    <span style="color:'+cm+'">// '+t.n+' theme</span>'),
    ln(11,cm,'    <span style="color:'+kw+'">colors</span> := <span style="color:'+pa+'">[]</span><span style="color:'+kw+'">string</span><span style="color:'+pa+'">{</span>'),
    ln(12,cm,'        <span style="color:'+st+'">"'+P[1].h+'"</span>, <span style="color:'+st+'">"'+P[2].h+'"</span>, <span style="color:'+st+'">"'+P[4].h+'"</span>,'),
    ln(13,cm,'    <span style="color:'+pa+'">}</span>'),
    ln(14,cm,'    <span style="color:'+kw+'">for</span> <span style="color:'+kw+'">_</span>, <span style="color:'+kw+'">c</span> := <span style="color:'+kw+'">range</span> <span style="color:'+kw+'">colors</span> <span style="color:'+pa+'">{</span>'),
    ln(15,cm,'        <span style="color:'+fn+'">fmt</span>.<span style="color:'+fn+'">Println</span>(<span style="color:'+kw+'">c</span>)'),
    ln(16,cm,'    <span style="color:'+pa+'">}</span>'),
    ln(17,cm,'<span style="color:'+pa+'">}</span>'),
  ];
  code.innerHTML=lines.join('');

  // Palette swatches + meta
  let pal='<div class="panel-palette">';
  for(let i=0;i<16;i++)pal+='<div class="mini-swatch" style="background:'+P[i].h+'"></div>';
  pal+='</div>';
  const border=dk?'rgba(255,255,255,.08)':'rgba(0,0,0,.08)';
  meta.style.borderColor=border;
  meta.innerHTML='<span class="meta-item">FG/BG <span class="meta-val">'+t.fc.toFixed(1)+':1 '+t.fl+'</span></span>'+
    '<span class="meta-item">AA <span class="meta-val">'+t.a+'/16</span></span>'+
    '<span class="meta-item">AAA <span class="meta-val">'+t.A+'/16</span></span>'+pal;
}

function ln(n,cm,code){
  return '<div class="line"><span class="ln" style="color:'+cm+'">'+n+'</span>'+code+'</div>';
}
function sel(n,cm,bg,fg,code){
  return '<div class="line"><span class="sel" style="background:'+bg+';color:'+fg+'"><span class="ln" style="color:'+cm+'">'+n+'</span>'+code+'</span></div>';
}

function isDark(hex){
  const r=parseInt(hex.slice(1,3),16),g=parseInt(hex.slice(3,5),16),b=parseInt(hex.slice(5,7),16);
  return(0.2126*r+0.7152*g+0.0722*b)/255<0.5;
}

function pick(theme){
  winners.push(theme);
  matchesPlayed++;
  // Brief flash then next
  setTimeout(nextMatch,120);
}

function showWinner(t){
  champion=t;
  document.getElementById('arena').style.display='none';
  document.getElementById('progressWrap').style.display='none';
  document.getElementById('roundInfo').textContent='';
  const ws=document.getElementById('winnerScreen');
  ws.classList.add('show');
  document.getElementById('winnerName').textContent=t.n;

  // Render winner code preview
  const dk=isDark(t.b);
  const wp=document.getElementById('winnerPreview');
  wp.style.background=t.b;wp.style.color=t.f;
  const wc=document.getElementById('winnerCode');
  renderPanel('Winner',t);
  // Reuse panel render into winner
  const P=t.p;
  const kw=P[4].h,st=P[2].h,nm=P[1].h,cm=P[8].h,fn=P[6].h,pa=P[7].h;
  const cB=t.c?t.c.x:t.f,cF=t.ct?t.ct.x:t.b;
  const sB=t.sl?t.sl.y:dk?'rgba(255,255,255,.12)':'rgba(0,0,0,.1)';
  const sF=t.sl?t.sl.x:t.f;
  wc.innerHTML=[
    ln(1,cm,'<span style="color:'+kw+'">package</span> <span style="color:'+fn+'">main</span>'),
    ln(2,cm,''),
    ln(3,cm,'<span style="color:'+kw+'">import</span> <span style="color:'+st+'">"fmt"</span>'),
    ln(4,cm,''),
    sel(5,cm,sB,sF,'<span style="color:'+kw+'">func</span> <span style="color:'+fn+'">main</span><span style="color:'+pa+'">()</span> <span style="color:'+pa+'">{</span>'),
    sel(6,cm,sB,sF,'    <span style="color:'+kw+'">msg</span> := <span style="color:'+st+'">"You chose '+t.n+'!"</span>'),
    ln(7,cm,'    <span style="color:'+fn+'">fmt</span>.<span style="color:'+fn+'">Println</span>(<span style="color:'+kw+'">msg</span><span class="cur" style="background:'+cB+';color:'+cF+'">)</span>'),
    ln(8,cm,'<span style="color:'+pa+'">}</span>'),
  ].join('');

  document.getElementById('winnerStats').innerHTML=
    '<span>Score: <strong>'+Math.round(t.s)+'</strong></span>'+
    '<span>FG/BG: <strong>'+t.fc.toFixed(1)+':1 '+t.fl+'</strong></span>'+
    '<span>AA: <strong>'+t.a+'/16</strong></span>'+
    '<span>AAA: <strong>'+t.A+'/16</strong></span>'+
    '<span>Rounds: <strong>'+round+'</strong></span>';
}

function copyThemeName(){
  if(champion)navigator.clipboard.writeText(champion.n).then(()=>{
    const btn=event.target;btn.textContent='Copied!';
    setTimeout(()=>btn.textContent='Copy Theme Name',1500);
  });
}

// Click/keyboard handlers for panels
document.getElementById('panelL').addEventListener('click',()=>pick(currentL));
document.getElementById('panelR').addEventListener('click',()=>pick(currentR));
document.addEventListener('keydown',(e)=>{
  if(!currentL||!currentR)return;
  if(document.getElementById('arena').style.display==='none')return;
  if(e.key==='ArrowLeft'||e.key==='1'){pick(currentL);e.preventDefault()}
  if(e.key==='ArrowRight'||e.key==='2'){pick(currentR);e.preventDefault()}
});
</script>
</body>
</html>`

	outPath := filepath.Join(outDir, "picker.html")
	os.WriteFile(outPath, []byte(html), 0644)
	fmt.Fprintf(os.Stderr, "Picker: %s (%d bytes)\n", outPath, len(html))
}
