package benchmarking

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	client "bank-application/internal/client"
)

var peers = map[int32]string{
	1: "localhost:50051",
	2: "localhost:50052",
	3: "localhost:50053",
	4: "localhost:50054",
	5: "localhost:50055",
	6: "localhost:50056",
	7: "localhost:50057",
	8: "localhost:50058",
	9: "localhost:50059",
}

func newClientManager() *client.ClientManager {
	return client.NewClientManager(peers)
}

func RunBenchmark(cfg BenchmarkConfig) {
	rand.Seed(time.Now().UnixNano())

	FlushBeforeBenchmark(peers)

	cm := newClientManager()
	metrics := &Metrics{}

	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.DurationSec)*time.Second)
	defer cancel()

	startWall := time.Now()
	var totalOps int64
	var txnLimit int64
	if cfg.Transactions > 0 {
		txnLimit = int64(cfg.Transactions)
	}

	//stop := time.After(time.Duration(cfg.DurationSec) * time.Second)

	for w := 0; w < cfg.Concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for {
				if txnLimit > 0 && atomic.LoadInt64(&totalOps) >= txnLimit {
					return
				}
				select {
				case <-ctx.Done():
					return
				default:
					txn := GenerateOp(cfg)
					if txn == nil {
						continue
					}

					start := time.Now()
					var ok bool

					if txn.Command.Type == client.CommandTypeRead {
						client1, ok1 := cm.Clients[txn.Sender]
						if !ok1 {
							continue
						}
						ok = client1.SendRead(txn, peers)
					} else if txn.Command.Type == client.CommandTypeTransfer {
						client1, ok1 := cm.Clients[txn.Sender]
						if !ok1 {
							continue
						}
						ok = client1.SendTransaction(txn, peers)
					} else {
						continue
					}

					lat := time.Since(start)
					metrics.Add(ok, lat)

					newCount := atomic.AddInt64(&totalOps, 1)
					if txnLimit > 0 && newCount >= txnLimit {
						cancel()
						return
					}
				}
			}
		}(w)
	}

	wg.Wait()

	s := metrics.Summary()
	if s.Ops == 0 {
		log.Printf("[Benchmark DONE] No operations executed")
		return
	}

	elapsed := time.Since(startWall)
	if elapsed <= 0 {
		elapsed = time.Millisecond
	}
	throughput := float64(s.Ops) / elapsed.Seconds()

	log.Printf("[Benchmark DONE]")
	log.Printf("Total Ops:   %d", s.Ops)
	log.Printf("Success:     %d", s.Ok)
	log.Printf("Failed:      %d", s.Fail)
	log.Printf("Avg Latency: %.2f ms", s.AvgLatMs)
	log.Printf("p50 Latency: %.2f ms", s.P50LatMs)
	log.Printf("p90 Latency: %.2f ms", s.P90LatMs)
	log.Printf("p99 Latency: %.2f ms", s.P99LatMs)
	log.Printf("Throughput:  %.2f ops/sec", throughput)
}
