package client

import (
	"log"
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
	// 1) Build interaction counts: inter[account][cluster] = interaction count
	inter := make(map[int][numClusters + 1]int)

	log.Printf("Length of AllExecutedTransferTransactions is: %d", len(cm.AllExecutedTransferTransactions))
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

		sCluster := GetClusterIDForClient(int32(sid))
		rCluster := GetClusterIDForClient(int32(rid))
		if sCluster == 0 || rCluster == 0 {
			continue
		}

		// sender interacts with receiver's cluster
		sVec := inter[sid]
		sVec[rCluster]++
		inter[sid] = sVec

		// receiver interacts with sender's cluster
		rVec := inter[rid]
		rVec[sCluster]++
		inter[rid] = rVec
	}

	for acc := 1; acc <= totalAccounts; acc++ {
		if _, ok := inter[acc]; !ok {
			inter[acc] = [numClusters + 1]int{}
		}
	}

	// 2) For each account, compute current cluster, best cluster and gain
	type accPref struct {
		Account     int
		FromCluster int
		ToCluster   int
		Gain        int
	}

	var prefs []accPref
	clusterSizes := make([]int, numClusters+1)

	for acc := 1; acc <= totalAccounts; acc++ {
		curr := GetClusterIDForClient(int32(acc))
		if curr == 0 {
			continue
		}
		clusterSizes[curr]++

		vec := inter[acc]

		bestCluster := curr
		bestCount := vec[curr]

		for c := 1; c <= numClusters; c++ {
			if vec[c] > bestCount {
				bestCount = vec[c]
				bestCluster = c
			}
		}

		gain := bestCount - vec[curr]

		prefs = append(prefs, accPref{
			Account:     acc,
			FromCluster: curr,
			ToCluster:   bestCluster,
			Gain:        gain,
		})
	}

	// 3) Sort accounts by gain descending (higher benefit to move first)
	sort.Slice(prefs, func(i, j int) bool {
		if prefs[i].Gain != prefs[j].Gain {
			return prefs[i].Gain > prefs[j].Gain
		}
		return prefs[i].Account < prefs[j].Account
	})

	newCluster := make([]int, totalAccounts+1)
	for acc := 1; acc <= totalAccounts; acc++ {
		newCluster[acc] = GetClusterIDForClient(int32(acc))
	}

	// 4) Greedy move with capacity constraints [minClusterSize, maxClusterSize]
	for _, p := range prefs {
		if p.ToCluster == p.FromCluster {
			continue // no change
		}
		from := p.FromCluster
		to := p.ToCluster

		if clusterSizes[to] >= maxClusterSize {
			continue
		}
		if clusterSizes[from] <= minClusterSize {
			continue
		}

		newCluster[p.Account] = to
		clusterSizes[from]--
		clusterSizes[to]++
	}

	// 5) Collect all moves where newCluster differs from original
	var moves []ReshardMove
	for acc := 1; acc <= totalAccounts; acc++ {
		from := GetClusterIDForClient(int32(acc))
		to := newCluster[acc]
		if from == 0 || to == 0 {
			continue
		}
		if from != to {
			moves = append(moves, ReshardMove{
				Account:     acc,
				FromCluster: from,
				ToCluster:   to,
			})
		}
	}

	// stable order: sort by account id
	sort.Slice(moves, func(i, j int) bool {
		return moves[i].Account < moves[j].Account
	})

	return moves
}
