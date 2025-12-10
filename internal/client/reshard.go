package client

import (
	"bank-application/internal/database"
	"sort"
	"strconv"
)

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

	cluster := make([]int, totalAccounts+1)
	clusterSize := make([]int, numClusters+1)

	for acc := 1; acc <= totalAccounts; acc++ {
		c, _ := database.GetShardMapping(acc)
		if c < 1 || c > numClusters {
			continue
		}
		cluster[acc] = c
		clusterSize[c]++
	}

	maxPasses := 5
	for pass := 0; pass < maxPasses; pass++ {
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

			for targetC := 1; targetC <= numClusters; targetC++ {
				if targetC == curC {
					continue
				}

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

					if nbC == curC && nbC != targetC {
						delta += w
					}
					if nbC != curC && nbC == targetC {
						delta -= w
					}
				}

				if delta < bestDelta {
					bestDelta = delta
					bestC = targetC
				}
			}

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

	var moves []ReshardMove

	for acc := 1; acc <= totalAccounts; acc++ {
		orig, _ := database.GetShardMapping(acc)
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
