THEMES_DIR ?= /Applications/Ghostty.app/Contents/Resources/ghostty/themes
DIST        = dist

.PHONY: all html csv clean serve open open-picker

all: html csv

html: $(DIST)/index.html $(DIST)/picker.html

csv: $(DIST)/report.csv

$(DIST)/index.html $(DIST)/picker.html: generate_report_html.go
	go run generate_report_html.go $(THEMES_DIR) $(DIST)

$(DIST)/report.csv: analyze_accessibility.go
	@mkdir -p $(DIST)
	go run analyze_accessibility.go $(THEMES_DIR) $(DIST)

serve: all
	@echo "Serving at http://localhost:8080"
	@cd $(DIST) && python3 -m http.server 8080

open: all
	open $(DIST)/index.html

open-picker: html
	open $(DIST)/picker.html

clean:
	rm -rf $(DIST)
