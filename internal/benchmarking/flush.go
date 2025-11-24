package benchmarking

import (
	"context"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "bank-application/pb/bank-application/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func FlushBeforeBenchmark(peers map[int32]string) {
	log.Println("[Benchmark] Flushing previous data on all nodes...")
	
	liveNodes := []string{"n1", "n2", "n3", "n4", "n5", "n6", "n7", "n8", "n9"}

	var liveNodeIDs []int32
	for _, ln := range liveNodes {
		ln = strings.TrimSpace(ln)
		id, _ := strconv.Atoi(strings.TrimPrefix(ln, "n"))
		liveNodeIDs = append(liveNodeIDs, int32(id))
	}

	var wg sync.WaitGroup

	for nodeID, addr := range peers {
		wg.Add(1)

		go func(nodeID int32, addr string) {
			defer wg.Done()

			conn, err := grpc.NewClient(
				addr,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			if err != nil {
				log.Printf("[Benchmark][Flush] Node %d connect failed: %v", nodeID, err)
				return
			}
			defer conn.Close()

			client := pb.NewBankApplicationClient(conn)

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			_, err = client.FlushPreviousDataAndUpdatePeersStatus(ctx,
				&pb.FlushAndUpdateStatusRequest{
					IsAlive:   true,
					LiveNodes: liveNodeIDs,
				},
			)

			if err != nil {
				log.Printf("[Benchmark][Flush] Node %d failed to update: %v", nodeID, err)
			} else {
				log.Printf("[Benchmark][Flush] Node %d flushed and marked alive", nodeID)
			}

		}(nodeID, addr)
	}

	wg.Wait()

	log.Println("[Benchmark] Finished flushing all node state. Sleeping 3 seconds...")
	time.Sleep(3 * time.Second)
}
