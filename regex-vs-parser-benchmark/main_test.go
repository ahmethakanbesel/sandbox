package main

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	"golang.org/x/net/html"
)

const (
	groundTruth = "https://titck.gov.tr/storage/Archive/2022/kubKtAttachments/kb_f807d1d0-a765-44ff-9b3f-b0cc3707fa54.pdf"
	shortInput  = `<div class="cell text-center"><a href="https://titck.gov.tr/storage/Archive/2022/kubKtAttachments/kb_f807d1d0-a765-44ff-9b3f-b0cc3707fa54.pdf" class="badge" target="_blank">PDF</a></div>`
)

func ExtractLinkByParsing(htmlContent string) (string, error) {
	reader := strings.NewReader(htmlContent)

	tokenizer := html.NewTokenizer(reader)

	for {
		tokenType := tokenizer.Next()
		switch tokenType {
		case html.ErrorToken:
			return "", fmt.Errorf("error parsing HTML")
		case html.StartTagToken, html.SelfClosingTagToken:
			token := tokenizer.Token()

			if token.Data == "a" {
				for _, attr := range token.Attr {
					if attr.Key == "href" {
						return attr.Val, nil
					}
				}
			}
		}
	}
}

func TestExtractLinkByParsing(t *testing.T) {
	link, err := ExtractLinkByParsing(shortInput)
	if err != nil {
		t.Fatal(err)
	}

	if link != groundTruth {
		t.Fatalf("expected %s, got %s", groundTruth, link)
	}
}

func ExtractLinkByRegex(htmlContent string) (string, error) {
	const pattern = `href="([^"]+\.pdf)`
	re := regexp.MustCompile(pattern)

	matches := re.FindStringSubmatch(htmlContent)

	if len(matches) < 2 {
		return "", fmt.Errorf("PDF link not found in the HTML")
	}

	return matches[1], nil
}

func TestExtractLinkByRegex(t *testing.T) {
	link, err := ExtractLinkByRegex(shortInput)
	if err != nil {
		t.Fatal(err)
	}

	if link != groundTruth {
		t.Fatalf("expected %s, got %s", groundTruth, link)
	}
}

func BenchmarkExtractLinkByParsing(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ExtractLinkByParsing(shortInput)
	}
}

func BenchmarkExtractLinkByRegex(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ExtractLinkByRegex(shortInput)
	}
}
