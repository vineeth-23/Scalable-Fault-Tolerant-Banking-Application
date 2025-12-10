package benchmarking

import (
	"flag"
	"fmt"
	"log"
	"math"
)

type BenchmarkConfig struct {
	DurationSec  int
	Concurrency  int
	Transactions int
	ReadOnly     float64
	IntraShard   float64
	CrossShard   float64
	Skew         float64
}

func ParseFlags() BenchmarkConfig {
	duration := flag.Int("duration", 10000, "benchmark duration in seconds")
	concurrency := flag.Int("concurrency", 5, "number of concurrent workers")
	transactions := flag.Int("transactions", 0, "max total transactions to run (0 = unlimited)")

	readOnly := flag.Float64("read-only", 0.7, "fraction of read-only transactions")
	intra := flag.Float64("intra-shard", 0.2, "fraction of intra-shard write transactions")
	cross := flag.Float64("cross-shard", 0.1, "fraction of cross-shard write transactions")

	skew := flag.Float64("skew", 0.0, "skew for key popularity: 0 = uniform, 1 = all hotkeys")

	flag.Parse()

	sum := *readOnly + *intra + *cross
	if math.Abs(sum-1.0) > 1e-6 {
		log.Fatalf("read-only + intra-shard + cross-shard MUST equal 1. got %.4f", sum)
	}

	if *skew < 0 || *skew > 1 {
		log.Fatalf("skew must be in [0,1], got %.3f", *skew)
	}

	cfg := BenchmarkConfig{
		DurationSec:  *duration,
		Concurrency:  *concurrency,
		Transactions: *transactions,
		ReadOnly:     *readOnly,
		IntraShard:   *intra,
		CrossShard:   *cross,
		Skew:         *skew,
	}

	fmt.Printf(
		"[Benchmark Config] duration=%ds conc=%d txns=%d read-only=%.2f intra=%.2f cross=%.2f skew=%.2f\n",
		cfg.DurationSec, cfg.Concurrency, cfg.Transactions, cfg.ReadOnly, cfg.IntraShard, cfg.CrossShard, cfg.Skew,
	)

	return cfg
}
