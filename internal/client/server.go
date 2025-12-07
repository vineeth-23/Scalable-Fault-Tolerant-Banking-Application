package client

import (
	pb "bank-application/pb/bank-application/pb"
	"context"
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
	thr, avg, ops := LastPerformance()
	return &pb.PerformanceResponse{
		Throughput:   thr,
		AvgLatencyMs: avg,
		Ops:          int32(ops),
	}, nil
}
