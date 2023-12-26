package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"
	"titck-scraper/pdfextractor"
)

func main() {
	var (
		inputPath   string
		outputPath  string
		workerCount int
	)

	flag.StringVar(&inputPath, "inputPath", "./docs/pdf", "Path to input PDF files")
	flag.StringVar(&outputPath, "outputPath", "./docs/txt", "Path to output TXT files")
	flag.IntVar(&workerCount, "workerCount", 10, "Number of worker goroutines")

	flag.Parse()

	inputPath, err := filepath.Abs(inputPath)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	outputPath, err = filepath.Abs(outputPath)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	extractor := pdfextractor.New(
		pdfextractor.WithWorkerCount(workerCount),
		pdfextractor.WithInputPath(inputPath),
		pdfextractor.WithOutputPath(outputPath),
	)

	startTime := time.Now()
	extractor.Run()
	endTime := time.Now()

	diff := endTime.Sub(startTime)
	slog.Info("extraction completed", "duration", diff.Seconds())
}
