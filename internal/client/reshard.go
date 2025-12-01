package client

import (
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

// ComputeReshardMoves builds a weighted graph of account interactions from
// AllExecutedTransferTransactions and then greedily moves accounts between
// clusters to reduce the global cross-shard cut, subject to size bounds.
func (cm *ClientManager) ComputeReshardMoves() []ReshardMove {
	// 1) Build adjacency: adj[u][v] = weight of transfers between u and v
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

	// 2) Initial assignment: range-based sharding
	cluster := make([]int, totalAccounts+1)
	clusterSize := make([]int, numClusters+1)

	for acc := 1; acc <= totalAccounts; acc++ {
		c := GetClusterIDForClient(int32(acc))
		if c < 1 || c > numClusters {
			continue
		}
		cluster[acc] = c
		clusterSize[c]++
	}

	// 3) Local search: move accounts between clusters to reduce cut
	maxPasses := 5
	for pass := 0; pass < maxPasses; pass++ {
		improved := false

		for acc := 1; acc <= totalAccounts; acc++ {
			neighbors := adj[acc]
			if len(neighbors) == 0 {
				continue // no edges, no effect on cut
			}

			curC := cluster[acc]
			if curC == 0 {
				continue
			}

			bestC := curC
			bestDelta := 0 // negative = improvement

			// try moving acc to each other cluster
			for targetC := 1; targetC <= numClusters; targetC++ {
				if targetC == curC {
					continue
				}
				// capacity constraints
				if clusterSize[targetC] >= maxClusterSize {
					continue
				}
				if clusterSize[curC] <= minClusterSize {
					continue
				}

				delta := 0
				for nb, w := range neighbors {
					nbC := cluster[nb]
					if nbC == 0 {
						continue
					}
					// edge contribution:
					// currently cross if nbC != curC
					// after move, cross if nbC != targetC

					// intra -> cross
					if nbC == curC && nbC != targetC {
						delta += w
					}
					// cross -> intra
					if nbC != curC && nbC == targetC {
						delta -= w
					}
				}

				if delta < bestDelta {
					bestDelta = delta
					bestC = targetC
				}
			}

			// apply best move if it improves the cut
			if bestC != curC {
				cluster[acc] = bestC
				clusterSize[curC]--
				clusterSize[bestC]++
				improved = true
			}
		}

		if !improved {
			break
		}
	}

	// 4) Collect moves relative to original range-based assignment
	var moves []ReshardMove
	for acc := 1; acc <= totalAccounts; acc++ {
		orig := GetClusterIDForClient(int32(acc))
		newC := cluster[acc]
		if orig == 0 || newC == 0 {
			continue
		}
		if orig != newC {
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

	return moves
}
