package main

import (
	"context"
	"encoding/binary"
	"flag"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/go-zeromq/zmq4"
)

const (
	tcpAddress = "tcp://127.0.0.1:5555"
	ipcAddress = "ipc:///tmp/benchmark.ipc"
	msgCount   = 100_000
)

func main() {
	useIPC := flag.Bool("use-ipc", false, "Use IPC instead of TCP")
	flag.Parse()

	address := tcpAddress
	if *useIPC {
		address = ipcAddress
		cleanupIPCSocket(ipcAddress)
	}

	log.Printf("Running publisher with GOMAXPROCS = %d", runtime.GOMAXPROCS(0))
	log.Printf("Using address: %s", address)

	ctx := context.Background()
	publisher := zmq4.NewPub(ctx)
	defer publisher.Close()

	if err := publisher.Listen(address); err != nil {
		log.Fatalf("Failed to listen on publisher socket: %v", err)
	}
	log.Printf("Publisher is listening on %s", address)

	time.Sleep(time.Second) // Give subscriber time to connect

	log.Println("Starting to publish messages...")
	for i := 0; i < msgCount; i++ {
		timestamp := time.Now().UTC().UnixNano()
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(timestamp))
		msg := zmq4.NewMsg(buf)

		if err := publisher.Send(msg); err != nil {
			log.Printf("Failed to send message: %v", err)
			continue
		}

		if i%100 == 0 {
			log.Printf("Progress: %d/%d", i, msgCount)
		}

		time.Sleep(time.Millisecond) // 1000 messages per second
	}

	log.Println("Finished publishing messages")
	time.Sleep(time.Second) // Give subscriber time to process last messages

	if *useIPC {
		cleanupIPCSocket(ipcAddress)
	}
}

func cleanupIPCSocket(address string) {
	// Extract the file path from the IPC address
	socketPath := address[6:] // Remove "ipc://" prefix

	// Check if the file exists
	if _, err := os.Stat(socketPath); err == nil {
		// File exists, attempt to remove it
		if err := os.Remove(socketPath); err != nil {
			log.Printf("Warning: Failed to remove existing IPC socket file: %v", err)
		} else {
			log.Printf("Cleaned up existing IPC socket file: %s", socketPath)
		}
	}
}
