# ZeroMQ Benchmark

This benchmark is a simple test to measure the latency of ZeroMQ when sending and receiving messages between two processes on the same machine.

## System Details

- **OS**: Ubuntu 22.04 LTS (WSL2)

## Results

### TCP

```
2024/09/16 09:34:46 Average Latency: 232.947µs
2024/09/16 09:34:46 P99 Latency: 483.904µs
```

### IPC

```
2024/09/16 09:34:08 Average Latency: 280.595µs
2024/09/16 09:34:08 P99 Latency: 522.987µs
```