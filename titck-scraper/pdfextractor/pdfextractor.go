package pdfextractor

import (
	"context"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
)

type PDFExtractor struct {
	workerCount int
	inputPath   string
	outputPath  string
	wg          sync.WaitGroup
}

func New(options ...func(*PDFExtractor)) *PDFExtractor {
	e := &PDFExtractor{}
	for _, o := range options {
		o(e)
	}
	return e
}

func WithWorkerCount(count int) func(*PDFExtractor) {
	return func(e *PDFExtractor) {
		e.workerCount = count
	}
}

func WithInputPath(path string) func(*PDFExtractor) {
	return func(e *PDFExtractor) {
		e.inputPath = path
	}
}

func WithOutputPath(path string) func(*PDFExtractor) {
	return func(e *PDFExtractor) {
		e.outputPath = path
	}
}

func (e *PDFExtractor) Run() {
	ctx := context.TODO()
	filesChan := e.list(ctx)

	for i := 0; i < e.workerCount; i++ {
		e.wg.Add(1)
		go e.extract(ctx, filesChan)
	}

	e.wg.Wait()
}

func (e *PDFExtractor) list(ctx context.Context) <-chan string {
	filesChan := make(chan string)

	slog.Info("listing files", "inputPath", e.inputPath)

	go func() {
		defer close(filesChan)

		files, err := os.ReadDir(e.inputPath)
		if err != nil {
			slog.Error("list error", "error", err)
			return
		}

		for _, f := range files {

			if filepath.Ext(f.Name()) == ".pdf" {
				filesChan <- filepath.Join(e.inputPath, f.Name())
			}
		}
	}()

	return filesChan
}

func (e *PDFExtractor) extract(ctx context.Context, input <-chan string) {
	defer e.wg.Done()

	for pdfFile := range input {
		cmd := exec.Command("pdftotext", "-enc", "UTF-8", pdfFile)
		err := cmd.Run()
		if err != nil {
			slog.Error("extract error", "input", pdfFile, "error", err)
		} else {
			slog.Info("text extracted", "input", pdfFile)
		}

		txtFile := pdfFile[:len(pdfFile)-len(filepath.Ext(pdfFile))] + ".txt"
		outputPath := filepath.Join(e.outputPath, filepath.Base(txtFile))

		err = os.Rename(txtFile, outputPath)
		if err != nil {
			slog.Error("move error", "input", pdfFile, "error", err)
			continue
		}
	}
}
