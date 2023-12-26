package main

import (
	"flag"
	"log/slog"
	"time"
	"titck-scraper/scraper"
)

func main() {
	var (
		chunks      int
		limit       int
		workerCount int
		cookie      string
	)

	flag.IntVar(&chunks, "chunks", 100, "Number of chunks")
	flag.IntVar(&limit, "limit", 20_000, "Limit for data retrieval")
	flag.IntVar(&workerCount, "workerCount", 10, "Number of worker goroutines")
	flag.StringVar(&cookie, "cookie", "", "Session cookie (required)")

	flag.Parse()

	if cookie == "" {
		slog.Error("cookie flag is mandatory")
		return
	}

	scraper := scraper.New(
		scraper.WithChunks(chunks),
		scraper.WithLimit(limit),
		scraper.WithWorkerCount(workerCount),
		scraper.WithCookie(cookie),
	)

	startTime := time.Now()
	scraper.Run()
	endTime := time.Now()

	diff := endTime.Sub(startTime)
	slog.Info("download completed", "duration", diff.Seconds())
}
