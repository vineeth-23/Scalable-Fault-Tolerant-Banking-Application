package client

import (
	pb "bank-application/pb/bank-application/pb"
	"context"
	"sort"
	"time"
)

type ClientControlServer struct {
	pb.UnimplementedClientControlServer
	cm *ClientManager
}

func NewClientControlServer(cm *ClientManager) *ClientControlServer {
	return &ClientControlServer{cm: cm}
}

func (s *ClientControlServer) GetReshardPlan(
	ctx context.Context,
	req *pb.GetReshardPlanRequest,
) (*pb.GetReshardPlanResponse, error) {

	moves := s.cm.ComputeReshardMoves()

	resp := &pb.GetReshardPlanResponse{
		Moves: make([]*pb.ReshardMove, 0, len(moves)),
	}

	for _, m := range moves {
		resp.Moves = append(resp.Moves, &pb.ReshardMove{
			Account:     int32(m.Account),
			FromCluster: int32(m.FromCluster),
			ToCluster:   int32(m.ToCluster),
		})
	}

	return resp, nil
}

func (s *ClientControlServer) Performance(
	ctx context.Context,
	req *pb.PerformanceRequest,
) (*pb.PerformanceResponse, error) {
	perfMu.Lock()
	ops := perfOps
	lats := append([]time.Duration(nil), perfLats...)
	first := perfFirst
	last := perfLast
	start := perfStart
	perfMu.Unlock()

	throughput := 0.0
	if ops > 0 && !first.IsZero() && !last.IsZero() && last.After(first) {
		if secs := last.Sub(first).Seconds(); secs > 0 {
			throughput = float64(ops) / secs
		}
	} else {
		if secs := time.Since(start).Seconds(); secs > 0 {
			throughput = float64(ops) / secs
		}
	}
	throughput = UpdateTP(throughput)

	var p50, p95, p99 float64
	var avgMs float64
	if len(lats) > 0 {
		ms := make([]int64, len(lats))
		for i, d := range lats {
			ms[i] = d.Milliseconds()
		}
		sort.Slice(ms, func(i, j int) bool { return ms[i] < ms[j] })
		idx := func(p int) int {
			n := len(ms)
			r := (p*n + 99) / 100
			if r < 1 {
				r = 1
			}
			if r > n {
				r = n
			}
			return r - 1
		}
		p50 = float64(ms[idx(50)])
		p95 = float64(ms[idx(95)])
		p99 = float64(ms[idx(99)])
		var sum int64
		for _, v := range ms {
			sum += v
		}
		avgMs = float64(sum) / float64(len(ms))
	}

	return &pb.PerformanceResponse{
		Throughput:   throughput,
		AvgLatencyMs: avgMs,
		Ops:          int32(ops),
		P50LatencyMs: p50,
		P95LatencyMs: p95,
		P99LatencyMs: p99,
	}, nil
}
