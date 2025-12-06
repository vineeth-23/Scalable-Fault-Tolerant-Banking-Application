package main

import (
	"bank-application/internal/client"
	"bank-application/internal/database"
	"bank-application/pb/bank-application/pb"

	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func startClientControlServer(cm *client.ClientManager) {
	//lis, err := net.Listen("tcp", ":6000")
	lis, _ := net.Listen("tcp", "127.0.0.1:6000")

	//if err != nil {
	//	log.Printf("[ClientControl] failed to listen: %v", err)
	//	return
	//}

	grpcServer := grpc.NewServer()
	pb.RegisterClientControlServer(grpcServer, client.NewClientControlServer(cm))

	log.Println("[ClientControl] gRPC server listening on :6000")

	if err := grpcServer.Serve(lis); err != nil {
		log.Printf("[ClientControl] serve error: %v", err)
	}
}

func main() {
	sets := client.ParseCSV("C:/Users/skotha/Downloads/Proj-3_TC.csv")

	peers := map[int32]string{
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

	// Initialize Redis in the client process for persisting shard mappings
	database.InitRedisClient("localhost:6379")
	_ = database.IntializeShardMap(9000)

	cm := client.NewClientManager(peers)

	go startClientControlServer(cm)

	keys := make([]int, 0, len(sets))
	for k := range sets {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	for _, setNum := range keys {
		set := sets[setNum]
		fmt.Printf("\n=== Processing Set %d ===\n", setNum)
		fmt.Printf("Live nodes: %v\n", set.LiveNodes)

		fmt.Println("Press ENTER to run this set...")
		bufio.NewReader(os.Stdin).ReadBytes('\n')
		var wg sync.WaitGroup

		// Persist resharding plan after this set so subsequent sets use updated mapping
		moves := cm.ComputeReshardMoves()
		if len(moves) > 0 {
			upd := make(map[int]int, len(moves))
			for _, m := range moves {
				log.Printf("[Reshard] Move: Account=%d FromCluster=%d → ToCluster=%d",
					m.Account, m.FromCluster, m.ToCluster)
				upd[int(m.Account)] = int(m.ToCluster)
			}
			if err := database.BulkSetShardMappings(upd); err != nil {
				log.Printf("[Reshard] failed to persist shard mapping: %v", err)
			} else {
				log.Printf("[Reshard] persisted %d moves", len(moves))
				// Migrate balances for moved accounts to the new owning cluster
				for _, m := range moves {
					//bal, err := database.GetClusterBalance(int(m.FromCluster), int(m.Account))
					//log.Printf("[Reshard] Balance = %d, from_cluster = %d, client_id = %d", bal, int(m.FromCluster), int(m.Account))
					//if err != nil {
					//	log.Printf("[Reshard] warn: couldn't read balance for acc=%d from cluster=%d: %v", m.Account, m.FromCluster, err)
					//	// fall back to 10 if not found
					//	bal = 10
					//}
					var bal int32
					bal = 10
					if err := database.SetClusterBalance(int(m.ToCluster), int(m.Account), bal); err != nil {
						log.Printf("[Reshard] error: couldn't write balance for acc=%d to cluster=%d: %v", m.Account, m.ToCluster, err)
						continue
					}
					// Remove from old cluster to avoid duplication
					if err := database.DeleteClusterBalance(int(m.FromCluster), int(m.Account)); err != nil {
						log.Printf("[Reshard] warn: couldn't delete acc=%d from old cluster=%d: %v", m.Account, m.FromCluster, err)
					}
				}
			}
		}

		cm.Reset()

		for nodeID, addr := range peers {
			wg.Add(1)
			go func(nodeID int32, addr string) {
				defer wg.Done()
				conn, err := grpc.NewClient(
					addr,
					grpc.WithTransportCredentials(insecure.NewCredentials()),
				)
				if err != nil {
					return
				}
				defer conn.Close()

				node := pb.NewBankApplicationClient(conn)

				alive := false
				for _, live := range set.LiveNodes {
					if strings.TrimSpace(live) == fmt.Sprintf("n%d", nodeID) {
						alive = true
						break
					}
				}

				var liveNodeIDs []int32
				for _, live := range set.LiveNodes {
					live = strings.TrimSpace(live)
					if strings.HasPrefix(live, "n") {
						if id, err := strconv.Atoi(strings.TrimPrefix(live, "n")); err == nil {
							liveNodeIDs = append(liveNodeIDs, int32(id))
						}
					}
				}

				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_, err = node.FlushPreviousDataAndUpdatePeersStatus(ctx, &pb.FlushAndUpdateStatusRequest{IsAlive: alive, LiveNodes: liveNodeIDs})
			}(nodeID, addr)
		}
		wg.Wait()

		time.Sleep(3 * time.Second)

		cm.RunSet(set.Transactions)
	}
	fmt.Println("\nAll sets completed. Press ENTER to stop the client server...")
	bufio.NewReader(os.Stdin).ReadBytes('\n')
}
