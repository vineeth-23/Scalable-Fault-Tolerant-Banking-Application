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
	perfFirst time.Time
	perfLast  time.Time
)

func BeginPerformance() {
	perfMu.Lock()
	perfStart = time.Now()
	perfFirst = time.Time{}
	perfLast = time.Time{}
	perfLats = nil
	perfOps = 0
	perfMu.Unlock()
}

func RecordPerf(success bool, lat time.Duration) {
	if !success {
		return
	}
	now := time.Now()
	start := now.Add(-lat)
	perfMu.Lock()
	perfOps++
	perfLats = append(perfLats, lat)
	if perfFirst.IsZero() || start.Before(perfFirst) {
		perfFirst = start
	}
	if perfLast.IsZero() || now.After(perfLast) {
		perfLast = now
	}
	perfMu.Unlock()
}
