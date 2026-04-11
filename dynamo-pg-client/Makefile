.PHONY: test test-unit test-e2e lint coverage bench profile docker-up docker-down build clean

# Default target
all: lint test

# Build
build:
	go build ./...

# Docker
docker-up:
	docker compose up -d

docker-down:
	docker compose down

# Testing
test:
	go test ./... -count=1 -timeout 120s

test-unit:
	go test ./pgdynamo/... -count=1 -timeout 60s

test-e2e:
	go test . -count=1 -timeout 120s -v

# Coverage
coverage:
	go test ./... -count=1 -timeout 120s \
		-coverprofile=coverage.out \
		-coverpkg=./pgdynamo/...,./pgdynamo/expr/...,./pgdynamo/storage/...,./pgdynamo/operations/...
	go tool cover -func=coverage.out | grep total:
	go tool cover -html=coverage.out -o coverage.html

# Linting
lint:
	golangci-lint run ./...

lint-fix:
	golangci-lint run --fix ./...

# Benchmarks
bench:
	go test ./... -bench=. -benchmem -count=3 -timeout 300s | tee bench.txt

bench-cpu:
	go test . -bench=. -benchmem -cpuprofile=cpu.prof -timeout 300s
	@echo "Run: go tool pprof -http=:8080 cpu.prof"

bench-mem:
	go test . -bench=. -benchmem -memprofile=mem.prof -timeout 300s
	@echo "Run: go tool pprof -http=:8080 mem.prof"

# Profiling
profile-cpu:
	go test . -bench=BenchmarkPutItem -benchtime=5s -cpuprofile=cpu.prof -timeout 300s
	go tool pprof -top cpu.prof

profile-mem:
	go test . -bench=BenchmarkPutItem -benchtime=5s -memprofile=mem.prof -timeout 300s
	go tool pprof -top mem.prof

profile-trace:
	go test . -bench=BenchmarkPutItem -benchtime=2s -trace=trace.out -timeout 300s
	@echo "Run: go tool trace trace.out"

# Clean generated files
clean:
	rm -f coverage.out coverage.html cpu.prof mem.prof trace.out bench.txt
