package benchmarking

import (
	"math"
	"sort"
	"sync"
	"time"
)

type Metrics struct {
	mu        sync.Mutex
	ops       int
	ok        int
	fail      int
	latencies []time.Duration
}

func (m *Metrics) Add(success bool, lat time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ops++
	if success {
		m.ok++
	} else {
		m.fail++
	}
	m.latencies = append(m.latencies, lat)
}

type MetricsSummary struct {
	Ops      int
	Ok       int
	Fail     int
	AvgLatMs float64
	P50LatMs float64
	P90LatMs float64
	P99LatMs float64
}

func (m *Metrics) Summary() MetricsSummary {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.ops == 0 || len(m.latencies) == 0 {
		return MetricsSummary{}
	}

	lats := make([]time.Duration, len(m.latencies))
	copy(lats, m.latencies)
	sort.Slice(lats, func(i, j int) bool {
		return lats[i] < lats[j]
	})

	total := time.Duration(0)
	for _, d := range lats {
		total += d
	}

	n := len(lats)
	avgMs := float64(total.Milliseconds()) / float64(n)

	p50 := percentile(lats, 0.50)
	p90 := percentile(lats, 0.90)
	p99 := percentile(lats, 0.99)

	return MetricsSummary{
		Ops:      m.ops,
		Ok:       m.ok,
		Fail:     m.fail,
		AvgLatMs: avgMs,
		P50LatMs: float64(p50.Milliseconds()),
		P90LatMs: float64(p90.Milliseconds()),
		P99LatMs: float64(p99.Milliseconds()),
	}
}

func percentile(lats []time.Duration, p float64) time.Duration {
	if len(lats) == 0 {
		return 0
	}
	if p <= 0 {
		return lats[0]
	}
	if p >= 1 {
		return lats[len(lats)-1]
	}

	pos := p * float64(len(lats)-1)
	idx := int(math.Round(pos))
	if idx < 0 {
		idx = 0
	}
	if idx >= len(lats) {
		idx = len(lats) - 1
	}
	return lats[idx]
}
