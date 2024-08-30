package main

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"time"
)

func estimatePi(pointsPerGoroutine int, results chan<- int) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	pointsInside := 0

	for i := 0; i < pointsPerGoroutine; i++ {
		x := r.Float64()*2 - 1
		y := r.Float64()*2 - 1
		if x*x+y*y <= 1 {
			pointsInside++
		}
	}

	results <- pointsInside
}

func main() {
	start := time.Now()

	numCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(numCPU)

	totalPoints := 1000000000 // 1 billion points
	pointsPerGoroutine := totalPoints / numCPU

	results := make(chan int, numCPU)
	var wg sync.WaitGroup

	for i := 0; i < numCPU; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			estimatePi(pointsPerGoroutine, results)
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	totalPointsInside := 0
	for pointsInside := range results {
		totalPointsInside += pointsInside
	}

	piEstimate := 4 * float64(totalPointsInside) / float64(totalPoints)

	duration := time.Since(start)

	fmt.Printf("Estimated Pi: %v\n", piEstimate)
	fmt.Printf("Time taken: %v\n", duration)
	fmt.Printf("Goroutines used: %d\n", numCPU)
}
