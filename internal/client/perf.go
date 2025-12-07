package client

import (
	"sync"
	"time"
)

var (
	perfMu    sync.Mutex
	perfStart time.Time
	perfLats  []time.Duration
	perfOps   int
)

// BeginPerformance clears metrics and marks the start of the current set window
func BeginPerformance() {
	perfMu.Lock()
	perfStart = time.Now()
	perfLats = nil
	perfOps = 0
	perfMu.Unlock()
}

// RecordPerf records one transaction's end-to-end latency. Only successes are counted.
func RecordPerf(success bool, lat time.Duration) {
	if !success {
		return
	}
	perfMu.Lock()
	perfOps++
	perfLats = append(perfLats, lat)
	perfMu.Unlock()
}
