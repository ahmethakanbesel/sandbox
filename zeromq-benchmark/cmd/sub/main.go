package main

import (
	"context"
	"encoding/binary"
	"flag"
	"log"
	"runtime"
	"sort"
	"time"

	"github.com/go-zeromq/zmq4"
)

const (
	tcpAddress = "tcp://127.0.0.1:5555"
	ipcAddress = "ipc:///tmp/benchmark.ipc"
	msgCount   = 10_000
)

func main() {
	useIPC := flag.Bool("use-ipc", false, "Use IPC instead of TCP")
	flag.Parse()

	address := tcpAddress
	if *useIPC {
		address = ipcAddress
	}

	log.Printf("Running subscriber with GOMAXPROCS = %d", runtime.GOMAXPROCS(0))
	log.Printf("Using address: %s", address)

	ctx := context.Background()
	subscriber := zmq4.NewSub(ctx)
	defer subscriber.Close()

	if err := subscriber.SetOption(zmq4.OptionSubscribe, ""); err != nil {
		log.Fatalf("Failed to set subscriber option: %v", err)
	}

	log.Println("Trying to connect to publisher...")
	for {
		if err := subscriber.Dial(address); err != nil {
			log.Printf("Failed to connect to publisher, retrying in 500ms: %v", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		log.Println("Connected to publisher")
		break
	}

	var latencies []time.Duration
	log.Println("Starting to receive messages...")

	for i := 0; i < msgCount; i++ {
		msg, err := subscriber.Recv()
		if err != nil {
			log.Printf("Failed to receive message: %v", err)
			continue
		}

		receivedTime := time.Now().UTC().UnixNano()
		if len(msg.Bytes()) != 8 {
			log.Printf("Received message with unexpected length: %d", len(msg.Bytes()))
			continue
		}
		sentTime := int64(binary.LittleEndian.Uint64(msg.Bytes()))

		latency := time.Duration(receivedTime - sentTime)
		latencies = append(latencies, latency)

		if i%100 == 0 {
			log.Printf("Progress: %d/%d", i, msgCount)
		}
	}

	log.Println("Finished receiving messages")

	// Calculate statistics
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p99 := latencies[int(float64(len(latencies))*0.99)]

	var sum time.Duration
	for _, lat := range latencies {
		sum += lat
	}
	avg := sum / time.Duration(len(latencies))

	log.Printf("Average Latency: %v", avg)
	log.Printf("P99 Latency: %v", p99)

	time.Sleep(time.Second * 30)
}
