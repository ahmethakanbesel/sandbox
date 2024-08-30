# Monte Carlo Pi Estimation

This repository contains implementations of a Monte Carlo method to estimate the
value of π (pi) in both C++ and Go. The program demonstrates the use of
multi-threading/concurrency to parallelize the computation across multiple CPU
cores.

## Results

### C++

```
Estimated Pi: 3.14168
Time taken: 17.8643 seconds
Threads used: 8
```

### Go

```
Estimated Pi: 3.141621952
Time taken: 1.236329167s
Goroutines used: 8
```

### System Information

| Component        | Specification                                  |
| ---------------- | ---------------------------------------------- |
| Operating System | darwin 14.0                                    |
| CPU              | Apple M1, 8 cores                              |
| RAM              | 8.00 GB                                        |
| Go Version       | go1.22.3                                       |
| C++ Compiler     | Apple clang version 15.0.0 (clang-1500.0.40.1) |
