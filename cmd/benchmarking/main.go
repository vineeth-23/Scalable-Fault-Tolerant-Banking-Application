package main

import "bank-application/internal/benchmarking"

func main() {
	cfg := benchmarking.ParseFlags()
	benchmarking.RunBenchmark(cfg)
}
