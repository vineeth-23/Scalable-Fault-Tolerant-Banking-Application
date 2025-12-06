package main

import (
	"bank-application/internal/benchmarking"
	"bank-application/internal/database"
)

func main() {
	cfg := benchmarking.ParseFlags()
	database.InitRedisClient("localhost:6379")
	_ = database.IntializeShardMap(9000)

	benchmarking.RunBenchmark(cfg)
}
