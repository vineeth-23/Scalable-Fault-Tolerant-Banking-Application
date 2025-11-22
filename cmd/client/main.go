package main

import (
	"bank-application/internal/client"
	"bank-application/pb/bank-application/pb"
	"bufio"
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

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

	cm := client.NewClientManager(peers)

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
}
