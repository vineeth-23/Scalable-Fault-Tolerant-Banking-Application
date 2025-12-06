package client

import (
	"bank-application/internal/database"
	"log"
	"sort"
	"strconv"
)

// reshard.go (client package)

const (
	totalAccounts  = 9000
	numClusters    = 3
	minClusterSize = 2950
	maxClusterSize = 3050
)

type ReshardMove struct {
	Account     int
	FromCluster int
	ToCluster   int
}

func (cm *ClientManager) ComputeReshardMoves() []ReshardMove {
	//log.Println("[Reshard] START ComputeReshardMoves")

	// 1) Build adjacency
	adj := make(map[int]map[int]int)

	addEdge := func(a, b int) {
		if a == b {
			return
		}
		if adj[a] == nil {
			adj[a] = make(map[int]int)
		}
		if adj[b] == nil {
			adj[b] = make(map[int]int)
		}
		adj[a][b]++
		adj[b][a]++
	}

	log.Printf("[Reshard] Processing %d transactions", len(cm.AllExecutedTransferTransactions))

	for _, tx := range cm.AllExecutedTransferTransactions {
		if tx == nil {
			continue
		}
		if tx.Sender == "" || tx.Reciever == "" {
			continue
		}
		sid, err1 := strconv.Atoi(tx.Sender)
		rid, err2 := strconv.Atoi(tx.Reciever)
		if err1 != nil || err2 != nil {
			continue
		}
		if sid < 1 || sid > totalAccounts || rid < 1 || rid > totalAccounts {
			continue
		}

		addEdge(sid, rid)
	}

	//log.Printf("[Reshard] Adjacency built with %d accounts having edges", len(adj))

	// 2) Initial assignment
	cluster := make([]int, totalAccounts+1)
	clusterSize := make([]int, numClusters+1)

	//log.Println("[Reshard] Loading existing shard assignments")
	for acc := 1; acc <= totalAccounts; acc++ {
		c, _ := database.GetShardMapping(acc)
		if c < 1 || c > numClusters {
			continue
		}
		cluster[acc] = c
		clusterSize[c]++
	}
	//log.Printf("[Reshard] Initial cluster sizes = %+v", clusterSize)

	// 3) Local search
	maxPasses := 5
	for pass := 0; pass < maxPasses; pass++ {
		//log.Printf("[Reshard] Pass %d starting...", pass)
		improved := false

		for acc := 1; acc <= totalAccounts; acc++ {
			neighbors := adj[acc]
			if len(neighbors) == 0 {
				continue
			}

			curC := cluster[acc]
			if curC == 0 {
				continue
			}

			bestC := curC
			bestDelta := 0

			//log.Printf("[Reshard] Checking account %d (current cluster %d)", acc, curC)

			for targetC := 1; targetC <= numClusters; targetC++ {
				if targetC == curC {
					continue
				}

				if clusterSize[targetC] >= maxClusterSize {
					//log.Printf("[Reshard]   Skip target %d: full", targetC)
					continue
				}
				if clusterSize[curC] <= minClusterSize {
					//log.Printf("[Reshard]   Skip moving %d: cur cluster would go below min size", acc)
					continue
				}

				delta := 0
				for nb, w := range neighbors {
					nbC := cluster[nb]
					if nbC == 0 {
						continue
					}

					// intra → cross
					if nbC == curC && nbC != targetC {
						delta += w
					}
					// cross → intra
					if nbC != curC && nbC == targetC {
						delta -= w
					}
				}

				//log.Printf("[Reshard]   Trying move acc=%d from %d → %d, delta=%d",
				//	acc, curC, targetC, delta)

				if delta < bestDelta {
					bestDelta = delta
					bestC = targetC
				}
			}

			if bestC != curC {
				//log.Printf("[Reshard]   Applying move acc=%d from %d → %d (delta=%d)",
				//	acc, curC, bestC, bestDelta)

				cluster[acc] = bestC
				clusterSize[curC]--
				clusterSize[bestC]++
				improved = true
			}
		}

		//log.Printf("[Reshard] End pass %d, improved=%v, cluster sizes=%+v",
		//	pass, improved, clusterSize)

		if !improved {
			//log.Printf("[Reshard] No improvement, stopping passes")
			break
		}
	}

	// 4) Collect moves
	var moves []ReshardMove

	//log.Println("[Reshard] Collecting final moves")

	for acc := 1; acc <= totalAccounts; acc++ {
		orig, _ := database.GetShardMapping(acc)
		newC := cluster[acc]

		if orig == 0 || newC == 0 {
			continue
		}
		if orig != newC {
			//log.Printf("[Reshard] Move calculated: Acc=%d From=%d To=%d", acc, orig, newC)
			moves = append(moves, ReshardMove{
				Account:     acc,
				FromCluster: orig,
				ToCluster:   newC,
			})
		}
	}

	sort.Slice(moves, func(i, j int) bool {
		return moves[i].Account < moves[j].Account
	})

	//log.Printf("[Reshard] FINAL RESULT: %d moves generated", len(moves))
	//log.Println("[Reshard] END ComputeReshardMoves")

	return moves
}
