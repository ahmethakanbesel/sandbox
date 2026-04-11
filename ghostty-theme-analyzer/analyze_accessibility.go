// WCAG 2.2 Color Accessibility Analyzer for Ghostty terminal themes.
// Spec: https://www.w3.org/TR/WCAG22/
//
// Criteria checked:
//   SC 1.4.3  Contrast (Minimum)  — Level AA  — 4.5:1 normal text, 3:1 large text
//   SC 1.4.6  Contrast (Enhanced) — Level AAA — 7:1 normal text, 4.5:1 large text
//   SC 1.4.11 Non-text Contrast   — Level AA  — 3:1 UI components
//
// Formulas per WCAG 2.2 / G17 technique:
//   Relative luminance: L = 0.2126*R + 0.7152*G + 0.0722*B
//     where each channel: if sRGB <= 0.04045 then C/12.92 else ((C+0.055)/1.055)^2.4
//   Contrast ratio: (L1 + 0.05) / (L2 + 0.05)  (L1 = lighter)
package main

import (
	"encoding/csv"
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

type Color struct {
	R, G, B uint8
}

type OptColor struct {
	Color Color
	Set   bool
}

type Theme struct {
	BG, FG                 Color
	CursorColor, CursorTxt OptColor
	SelBG, SelFG           OptColor
	Palette                [16]Color
}

type ThemeResult struct {
	Name string
	Theme

	// FG/BG text contrast (WCAG 1.4.3)
	FGBGContrast float64

	// Palette vs BG (WCAG 1.4.3)
	PaletteBGContrast [16]float64
	AANormalCount     int // >=4.5:1
	AALargeCount      int // >=3:1
	AAANormalCount    int // >=7:1
	AAALargeCount     int // >=4.5:1

	// Cursor: color vs BG (WCAG 1.4.11 non-text, >=3:1)
	CursorBGContrast    float64
	CursorBGPassesNonTx bool
	HasCursor           bool

	// Cursor text: cursor-text vs cursor-color (WCAG 1.4.3)
	CursorTxtContrast float64
	CursorTxtPassesAA bool
	HasCursorTxt      bool

	// Selection: sel-fg vs sel-bg (WCAG 1.4.3)
	SelFGBGContrast float64
	SelFGBGPassesAA bool
	HasSelection    bool

	// Selection distinguishability: sel-bg vs bg (WCAG 1.4.11, >=3:1)
	SelBGDistContrast    float64
	SelBGDistPassesNonTx bool
	HasSelBG             bool

	Score float64

	// Extra metrics
	BlueLight  float64
	CCT        float64
	MinDeltaE  float64
	HueStdDev  float64
	APCAFgBg   float64
	APCAPass75 int
	APCAPass60 int
}

func parseHexColor(s string) (Color, error) {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "#")
	if len(s) != 6 {
		return Color{}, fmt.Errorf("invalid hex color: %q", s)
	}
	r, err := strconv.ParseUint(s[0:2], 16, 8)
	if err != nil {
		return Color{}, err
	}
	g, err := strconv.ParseUint(s[2:4], 16, 8)
	if err != nil {
		return Color{}, err
	}
	b, err := strconv.ParseUint(s[4:6], 16, 8)
	if err != nil {
		return Color{}, err
	}
	return Color{uint8(r), uint8(g), uint8(b)}, nil
}

func linearize(c uint8) float64 {
	s := float64(c) / 255.0
	if s <= 0.04045 {
		return s / 12.92
	}
	return math.Pow((s+0.055)/1.055, 2.4)
}

// sRGB → CIE XYZ (D65)
func srgbToXYZ(c Color) (X, Y, Z float64) {
	r, g, b := linearize(c.R), linearize(c.G), linearize(c.B)
	X = 0.4124564*r + 0.3575761*g + 0.1804375*b
	Y = 0.2126729*r + 0.7151522*g + 0.0721750*b
	Z = 0.0193339*r + 0.1191920*g + 0.9503041*b
	return
}

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

func deltaE76(c1, c2 Color) float64 {
	L1, a1, b1 := colorToLab(c1)
	L2, a2, b2 := colorToLab(c2)
	return math.Sqrt((L1-L2)*(L1-L2) + (a1-a2)*(a1-a2) + (b1-b2)*(b1-b2))
}

func blueLightIndex(c Color) float64 {
	r, g, b := linearize(c.R), linearize(c.G), linearize(c.B)
	sum := r + g + b
	if sum < 1e-10 {
		return 0.333
	}
	return b / sum
}

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

// APCA-W3 0.0.98G-4g
func apcaLinearize(c uint8) float64 {
	return math.Pow(float64(c)/255.0, 2.4)
}

func apcaLuminance(c Color) float64 {
	return 0.2126729*apcaLinearize(c.R) + 0.7151522*apcaLinearize(c.G) + 0.0721750*apcaLinearize(c.B)
}

func apcaSoftClamp(y float64) float64 {
	if y >= 0.022 {
		return y
	}
	return y + math.Pow(0.022-y, 1.414)
}

func apcaContrast(text, bg Color) float64 {
	txtY := apcaSoftClamp(apcaLuminance(text))
	bgY := apcaSoftClamp(apcaLuminance(bg))
	if math.Abs(bgY-txtY) < 0.0005 {
		return 0
	}
	var sapc, lc float64
	if bgY > txtY {
		sapc = (math.Pow(bgY, 0.56) - math.Pow(txtY, 0.57)) * 1.14
		if sapc < 0.1 {
			return 0
		}
		lc = (sapc - 0.027) * 100
	} else {
		sapc = (math.Pow(bgY, 0.65) - math.Pow(txtY, 0.62)) * 1.14
		if sapc > -0.1 {
			return 0
		}
		lc = (sapc + 0.027) * 100
	}
	return math.Round(lc*10) / 10
}

func relativeLuminance(c Color) float64 {
	return 0.2126*linearize(c.R) + 0.7152*linearize(c.G) + 0.0722*linearize(c.B)
}

func contrastRatio(c1, c2 Color) float64 {
	l1 := relativeLuminance(c1)
	l2 := relativeLuminance(c2)
	if l1 < l2 {
		l1, l2 = l2, l1
	}
	return (l1 + 0.05) / (l2 + 0.05)
}

// wcagLevel returns the highest WCAG text-contrast level met.
//   >= 7.0:1 → AAA  (SC 1.4.6 normal text / SC 1.4.3 large text enhanced)
//   >= 4.5:1 → AA   (SC 1.4.3 normal text / SC 1.4.6 large text)
//   >= 3.0:1 → AA-Large (SC 1.4.3 large text only)
//   < 3.0:1  → Fail
func wcagLevel(ratio float64) string {
	switch {
	case ratio >= 7.0:
		return "AAA"
	case ratio >= 4.5:
		return "AA"
	case ratio >= 3.0:
		return "AA-Large"
	default:
		return "Fail"
	}
}

// nonTextLevel checks SC 1.4.11 non-text contrast (>= 3:1).
func nonTextLevel(ratio float64) string {
	if ratio >= 3.0 {
		return "Pass"
	}
	return "Fail"
}

func colorHex(c Color) string {
	return fmt.Sprintf("#%02X%02X%02X", c.R, c.G, c.B)
}

func parseTheme(path string) (*Theme, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	t := &Theme{}
	hasBG, hasFG := false, false

	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])

		switch key {
		case "background":
			t.BG, err = parseHexColor(val)
			if err != nil {
				return nil, fmt.Errorf("background: %w", err)
			}
			hasBG = true
		case "foreground":
			t.FG, err = parseHexColor(val)
			if err != nil {
				return nil, fmt.Errorf("foreground: %w", err)
			}
			hasFG = true
		case "cursor-color":
			c, cerr := parseHexColor(val)
			if cerr == nil {
				t.CursorColor = OptColor{c, true}
			}
		case "cursor-text":
			c, cerr := parseHexColor(val)
			if cerr == nil {
				t.CursorTxt = OptColor{c, true}
			}
		case "selection-background":
			c, cerr := parseHexColor(val)
			if cerr == nil {
				t.SelBG = OptColor{c, true}
			}
		case "selection-foreground":
			c, cerr := parseHexColor(val)
			if cerr == nil {
				t.SelFG = OptColor{c, true}
			}
		case "palette":
			pparts := strings.SplitN(val, "=", 2)
			if len(pparts) != 2 {
				continue
			}
			idx, perr := strconv.Atoi(strings.TrimSpace(pparts[0]))
			if perr != nil || idx < 0 || idx > 15 {
				continue
			}
			c, cerr := parseHexColor(pparts[1])
			if cerr != nil {
				continue
			}
			t.Palette[idx] = c
		}
	}

	if !hasBG || !hasFG {
		return nil, fmt.Errorf("missing background or foreground")
	}
	return t, nil
}

func analyzeTheme(path string) (*ThemeResult, error) {
	t, err := parseTheme(path)
	if err != nil {
		return nil, err
	}

	r := &ThemeResult{
		Name:  filepath.Base(path),
		Theme: *t,
	}

	// 1. FG vs BG (WCAG 1.4.3 text contrast)
	r.FGBGContrast = contrastRatio(t.FG, t.BG)

	// 2. Palette colors vs BG (WCAG 1.4.3)
	for i := 0; i < 16; i++ {
		cr := contrastRatio(t.Palette[i], t.BG)
		r.PaletteBGContrast[i] = cr
		if cr >= 3.0 {
			r.AALargeCount++
		}
		if cr >= 4.5 {
			r.AANormalCount++
			r.AAALargeCount++
		}
		if cr >= 7.0 {
			r.AAANormalCount++
		}
	}

	// 3. Cursor-color vs BG (WCAG 1.4.11 non-text contrast, >=3:1)
	if t.CursorColor.Set {
		r.HasCursor = true
		r.CursorBGContrast = contrastRatio(t.CursorColor.Color, t.BG)
		r.CursorBGPassesNonTx = r.CursorBGContrast >= 3.0
	}

	// 4. Cursor-text vs cursor-color (WCAG 1.4.3 text readability)
	if t.CursorColor.Set && t.CursorTxt.Set {
		r.HasCursorTxt = true
		r.CursorTxtContrast = contrastRatio(t.CursorTxt.Color, t.CursorColor.Color)
		r.CursorTxtPassesAA = r.CursorTxtContrast >= 4.5
	}

	// 5. Selection-FG vs selection-BG (WCAG 1.4.3 text readability)
	if t.SelFG.Set && t.SelBG.Set {
		r.HasSelection = true
		r.SelFGBGContrast = contrastRatio(t.SelFG.Color, t.SelBG.Color)
		r.SelFGBGPassesAA = r.SelFGBGContrast >= 4.5
	}

	// 6. Selection-BG vs BG (WCAG 1.4.11 non-text contrast, >=3:1)
	if t.SelBG.Set {
		r.HasSelBG = true
		r.SelBGDistContrast = contrastRatio(t.SelBG.Color, t.BG)
		r.SelBGDistPassesNonTx = r.SelBGDistContrast >= 3.0
	}

	r.BlueLight = math.Round(blueLightIndex(t.BG)*1000) / 1000
	r.CCT = math.Round(colorTempCCT(t.BG))
	r.MinDeltaE = math.Round(paletteMinDeltaE(t.Palette)*100) / 100
	r.HueStdDev = math.Round(paletteHueStdDev(t.Palette)*100) / 100
	r.APCAFgBg = apcaContrast(t.FG, t.BG)
	for i := 0; i < 16; i++ {
		lc := math.Abs(apcaContrast(t.Palette[i], t.BG))
		if lc >= 60 {
			r.APCAPass60++
		}
		if lc >= 75 {
			r.APCAPass75++
		}
	}

	r.Score = computeScore(r)
	return r, nil
}

// computeScore produces a 0-100 accessibility score.
//
// Breakdown:
//   FG/BG contrast:                30 pts (WCAG 1.4.3)
//   Palette AA-normal passes:      25 pts (WCAG 1.4.3)
//   Palette AAA passes:            15 pts (WCAG 1.4.3)
//   Cursor visibility vs BG:       10 pts (WCAG 1.4.11)
//   Cursor text readability:        5 pts (WCAG 1.4.3)
//   Selection text readability:    10 pts (WCAG 1.4.3)
//   Selection distinguishability:   5 pts (WCAG 1.4.11)
func computeScore(r *ThemeResult) float64 {
	score := 0.0

	// FG/BG (30 pts): ratio mapped to 0-1, capped at 21:1
	score += math.Min(r.FGBGContrast/21.0, 1.0) * 30.0

	// Palette AA normal (25 pts): fraction of 16 colors
	score += (float64(r.AANormalCount) / 16.0) * 25.0

	// Palette AAA (15 pts): fraction of 16 colors
	score += (float64(r.AAANormalCount) / 16.0) * 15.0

	// Cursor vs BG (10 pts)
	if r.HasCursor {
		score += math.Min(r.CursorBGContrast/21.0, 1.0) * 10.0
	} else {
		score += 5.0 // neutral when absent
	}

	// Cursor text (5 pts)
	if r.HasCursorTxt {
		score += math.Min(r.CursorTxtContrast/21.0, 1.0) * 5.0
	} else {
		score += 2.5
	}

	// Selection text (10 pts)
	if r.HasSelection {
		score += math.Min(r.SelFGBGContrast/21.0, 1.0) * 10.0
	} else {
		score += 5.0
	}

	// Selection BG distinguishability (5 pts)
	if r.HasSelBG {
		score += math.Min(r.SelBGDistContrast/21.0, 1.0) * 5.0
	} else {
		score += 2.5
	}

	return score
}

func fmtContrast(ratio float64, has bool) string {
	if !has {
		return "N/A"
	}
	return fmt.Sprintf("%.2f:1", ratio)
}

func fmtLevel(ratio float64, has bool, nonText bool) string {
	if !has {
		return "N/A"
	}
	if nonText {
		return nonTextLevel(ratio)
	}
	return wcagLevel(ratio)
}

func fmtBool(b bool, has bool) string {
	if !has {
		return "N/A"
	}
	if b {
		return "Yes"
	}
	return "No"
}

func fmtColor(oc OptColor) string {
	if !oc.Set {
		return "N/A"
	}
	return colorHex(oc.Color)
}

func main() {
	themesDir := "/Applications/Ghostty.app/Contents/Resources/ghostty/themes"
	outDir := "."
	if len(os.Args) > 1 {
		themesDir = os.Args[1]
	}
	if len(os.Args) > 2 {
		outDir = os.Args[2]
		os.MkdirAll(outDir, 0755)
	}

	entries, err := os.ReadDir(themesDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error reading directory: %v\n", err)
		os.Exit(1)
	}

	var paths []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if strings.HasPrefix(name, ".") || strings.HasSuffix(name, ".go") ||
			strings.HasSuffix(name, ".csv") || strings.HasSuffix(name, ".md") {
			continue
		}
		paths = append(paths, filepath.Join(themesDir, name))
	}

	numWorkers := runtime.NumCPU()
	fmt.Fprintf(os.Stderr, "Analyzing %d themes with %d workers...\n", len(paths), numWorkers)

	jobs := make(chan string, len(paths))
	resultsCh := make(chan *ThemeResult, len(paths))
	var wg sync.WaitGroup

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range jobs {
				r, err := analyzeTheme(p)
				if err != nil {
					fmt.Fprintf(os.Stderr, "  SKIP %s: %v\n", filepath.Base(p), err)
					continue
				}
				resultsCh <- r
			}
		}()
	}

	for _, p := range paths {
		jobs <- p
	}
	close(jobs)

	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	var results []*ThemeResult
	for r := range resultsCh {
		results = append(results, r)
	}

	sort.Slice(results, func(i, j int) bool {
		if results[i].Score != results[j].Score {
			return results[i].Score > results[j].Score
		}
		return results[i].Name < results[j].Name
	})

	// Write CSV
	outPath := filepath.Join(outDir, "report.csv")
	f, err := os.Create(outPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error creating CSV: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	header := []string{
		"Rank", "Theme", "Score (0-100)",
		"BG", "FG",
		"FG/BG Contrast", "FG/BG SC-1.4.3/1.4.6 Level",
	}
	for i := 0; i < 16; i++ {
		header = append(header,
			fmt.Sprintf("Palette %d", i),
			fmt.Sprintf("P%d vs BG Contrast", i),
			fmt.Sprintf("P%d vs BG SC-1.4.3/1.4.6", i),
		)
	}
	header = append(header,
		"Palette SC-1.4.3 AA Normal (>=4.5:1)",
		"Palette SC-1.4.3 AA Large (>=3:1)",
		"Palette SC-1.4.6 AAA Normal (>=7:1)",
		"Palette SC-1.4.6 AAA Large (>=4.5:1)",
		"Cursor Color",
		"Cursor vs BG Contrast",
		"Cursor vs BG SC-1.4.11 (>=3:1)",
		"Cursor Text Color",
		"CursorTxt vs Cursor Contrast",
		"CursorTxt vs Cursor SC-1.4.3/1.4.6",
		"Selection BG",
		"Selection FG",
		"SelFG vs SelBG Contrast",
		"SelFG vs SelBG SC-1.4.3/1.4.6",
		"SelBG vs BG Contrast",
		"SelBG vs BG SC-1.4.11 (>=3:1)",
		"Blue Light Index",
		"Color Temperature (K)",
		"Palette Min DeltaE",
		"Palette Hue StdDev (deg)",
		"APCA FG/BG (Lc)",
		"APCA Palette Body (|Lc|>=75)",
		"APCA Palette Large (|Lc|>=60)",
	)
	w.Write(header)

	for rank, r := range results {
		row := []string{
			strconv.Itoa(rank + 1),
			r.Name,
			fmt.Sprintf("%.0f", math.Round(r.Score)),
			colorHex(r.BG), colorHex(r.FG),
			fmt.Sprintf("%.2f:1", r.FGBGContrast),
			wcagLevel(r.FGBGContrast),
		}
		for i := 0; i < 16; i++ {
			c := r.Palette[i]
			row = append(row,
				colorHex(c),
				fmt.Sprintf("%.2f:1", r.PaletteBGContrast[i]),
				wcagLevel(r.PaletteBGContrast[i]),
			)
		}
		row = append(row,
			fmt.Sprintf("%d/16", r.AANormalCount),
			fmt.Sprintf("%d/16", r.AALargeCount),
			fmt.Sprintf("%d/16", r.AAANormalCount),
			fmt.Sprintf("%d/16", r.AAALargeCount),
			fmtColor(r.CursorColor),
			fmtContrast(r.CursorBGContrast, r.HasCursor),
			fmtLevel(r.CursorBGContrast, r.HasCursor, true),
			fmtColor(r.CursorTxt),
			fmtContrast(r.CursorTxtContrast, r.HasCursorTxt),
			fmtLevel(r.CursorTxtContrast, r.HasCursorTxt, false),
			fmtColor(r.SelBG),
			fmtColor(r.SelFG),
			fmtContrast(r.SelFGBGContrast, r.HasSelection),
			fmtLevel(r.SelFGBGContrast, r.HasSelection, false),
			fmtContrast(r.SelBGDistContrast, r.HasSelBG),
			fmtLevel(r.SelBGDistContrast, r.HasSelBG, true),
			fmt.Sprintf("%.3f", r.BlueLight),
			fmt.Sprintf("%.0f", r.CCT),
			fmt.Sprintf("%.2f", r.MinDeltaE),
			fmt.Sprintf("%.2f", r.HueStdDev),
			fmt.Sprintf("%.1f", r.APCAFgBg),
			fmt.Sprintf("%d/16", r.APCAPass75),
			fmt.Sprintf("%d/16", r.APCAPass60),
		)
		w.Write(row)
	}

	fmt.Fprintf(os.Stderr, "Report written to %s (%d themes)\n\n", outPath, len(results))

	// Terminal summary
	fmt.Println("=== TOP 15 MOST ACCESSIBLE THEMES ===")
	fmt.Printf("%-4s %-30s %5s %9s %7s %5s %5s %8s %8s %8s\n",
		"Rank", "Theme", "Score", "FG/BG", "Level", "AA", "AAA", "Cursor", "SelText", "SelDist")
	fmt.Println(strings.Repeat("-", 105))
	top := 15
	if top > len(results) {
		top = len(results)
	}
	for i := 0; i < top; i++ {
		r := results[i]
		cur := fmtLevel(r.CursorBGContrast, r.HasCursor, true)
		sel := fmtLevel(r.SelFGBGContrast, r.HasSelection, false)
		selD := fmtLevel(r.SelBGDistContrast, r.HasSelBG, true)
		fmt.Printf("%-4d %-30s %5.1f %8.1f:1 %7s %4d/16 %4d/16 %8s %8s %8s\n",
			i+1, r.Name, r.Score, r.FGBGContrast, wcagLevel(r.FGBGContrast),
			r.AANormalCount, r.AAANormalCount, cur, sel, selD)
	}

	if len(results) > 5 {
		fmt.Println("\n=== BOTTOM 5 LEAST ACCESSIBLE THEMES ===")
		fmt.Printf("%-4s %-30s %5s %9s %7s %5s %5s %8s %8s %8s\n",
			"Rank", "Theme", "Score", "FG/BG", "Level", "AA", "AAA", "Cursor", "SelText", "SelDist")
		fmt.Println(strings.Repeat("-", 105))
		for i := len(results) - 5; i < len(results); i++ {
			r := results[i]
			cur := fmtLevel(r.CursorBGContrast, r.HasCursor, true)
			sel := fmtLevel(r.SelFGBGContrast, r.HasSelection, false)
			selD := fmtLevel(r.SelBGDistContrast, r.HasSelBG, true)
			fmt.Printf("%-4d %-30s %5.1f %8.1f:1 %7s %4d/16 %4d/16 %8s %8s %8s\n",
				i+1, r.Name, r.Score, r.FGBGContrast, wcagLevel(r.FGBGContrast),
				r.AANormalCount, r.AAANormalCount, cur, sel, selD)
		}
	}

	// Summary stats
	var aaFG, aaaFG, curPass, selPass, selDist int
	for _, r := range results {
		if r.FGBGContrast >= 4.5 {
			aaFG++
		}
		if r.FGBGContrast >= 7.0 {
			aaaFG++
		}
		if r.CursorBGPassesNonTx {
			curPass++
		}
		if r.HasSelection && r.SelFGBGPassesAA {
			selPass++
		}
		if r.SelBGDistPassesNonTx {
			selDist++
		}
	}
	total := len(results)
	fmt.Printf("\n=== SUMMARY (%d themes) ===\n", total)
	fmt.Printf("FG/BG passes AA (4.5:1):       %d/%d (%.0f%%)\n", aaFG, total, float64(aaFG)/float64(total)*100)
	fmt.Printf("FG/BG passes AAA (7:1):        %d/%d (%.0f%%)\n", aaaFG, total, float64(aaaFG)/float64(total)*100)
	fmt.Printf("Cursor visible (1.4.11, 3:1):  %d/%d\n", curPass, total)
	fmt.Printf("Selection text AA (4.5:1):     %d/%d\n", selPass, total)
	fmt.Printf("Selection distinct (1.4.11):   %d/%d\n", selDist, total)
}
