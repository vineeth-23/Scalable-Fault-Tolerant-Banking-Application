package client

import "sync"

var (
	setMu  sync.RWMutex
	perSet = make(map[int]SetSnapshot)
)

// SetSnapshot stores finalized metrics for a set
// Values are stable and can be retrieved any time later.
type SetSnapshot struct {
	Throughput   float64
	AvgLatencyMs float64
	Ops          int
}

// StoreSetPerformance saves a snapshot for the given set number.
func StoreSetPerformance(set int, thr float64, avgMs float64, ops int) {
	setMu.Lock()
	perSet[set] = SetSnapshot{Throughput: thr, AvgLatencyMs: avgMs, Ops: ops}
	setMu.Unlock()
}

// GetSetPerformance returns metrics for a specific set. ok=false if not present.
func GetSetPerformance(set int) (thr float64, avgMs float64, ops int, ok bool) {
	setMu.RLock()
	s, ok := perSet[set]
	setMu.RUnlock()
	if !ok {
		return 0, 0, 0, false
	}
	return s.Throughput, s.AvgLatencyMs, s.Ops, true
}
