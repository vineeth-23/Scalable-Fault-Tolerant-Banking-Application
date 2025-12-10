package client

import (
	"sync"
	"time"
)

var (
	snapMu   sync.RWMutex
	lastSnap PerfSnapshot
)

type PerfSnapshot struct {
	Throughput   float64
	AvgLatencyMs float64
	Ops          int
	StartedAt    time.Time
	EndedAt      time.Time
	DurationMs   int64
}

func EndPerformance() PerfSnapshot {
	perfMu.Lock()
	ops := perfOps
	lats := append([]time.Duration(nil), perfLats...)
	start := perfStart
	perfMu.Unlock()

	end := time.Now()
	elapsed := end.Sub(start)
	if elapsed <= 0 {
		elapsed = time.Millisecond
	}

	var avgMs float64
	if len(lats) > 0 {
		var sum time.Duration
		for _, d := range lats {
			sum += d
		}
		avgMs = float64(sum.Milliseconds()) / float64(len(lats))
	}

	thr := 0.0
	if elapsed.Seconds() > 0 {
		thr = float64(ops) / elapsed.Seconds()
	}

	snap := PerfSnapshot{
		Throughput:   thr,
		AvgLatencyMs: avgMs,
		Ops:          ops,
		StartedAt:    start,
		EndedAt:      end,
		DurationMs:   elapsed.Milliseconds(),
	}

	snapMu.Lock()
	lastSnap = snap
	snapMu.Unlock()
	return snap
}

func LastPerformance() (throughput float64, avgLatencyMs float64, ops int) {
	snapMu.RLock()
	defer snapMu.RUnlock()
	return lastSnap.Throughput, lastSnap.AvgLatencyMs, lastSnap.Ops
}
