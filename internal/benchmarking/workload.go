package benchmarking

import (
	"math/rand"
	"strconv"
	"time"

	client "bank-application/internal/client"
)

const (
	totalAccounts = 9000
	hotFraction   = 0.01 // 1 percent of keys are "hot"
)

var globalTime int32

func GetShard(id int32) int {
	switch {
	case id >= 1 && id <= 3000:
		return 1
	case id >= 3001 && id <= 6000:
		return 2
	case id >= 6001 && id <= 9000:
		return 3
	default:
		return 1
	}
}

func shardRange(s int) (int, int) {
	switch s {
	case 1:
		return 1, 3000
	case 2:
		return 3001, 6000
	case 3:
		return 6001, 9000
	default:
		return 1, totalAccounts
	}
}

func pickAccount(skew float64) int {
	if skew < 0 {
		skew = 0
	}
	if skew > 1 {
		skew = 1
	}

	hotSize := int(float64(totalAccounts) * hotFraction)
	if hotSize < 1 {
		hotSize = 1
	}

	if rand.Float64() < skew {
		// hot: [1, hotSize]
		return rand.Intn(hotSize) + 1
	}

	// uniform: [1, totalAccounts]
	return rand.Intn(totalAccounts) + 1
}

func GenerateOp(cfg BenchmarkConfig) *client.Txn {
	r := rand.Float64()

	if r < cfg.ReadOnly {
		acc := pickAccount(cfg.Skew)
		return &client.Txn{
			Sender: strconv.Itoa(acc),
			Command: client.Command{
				Type: client.CommandTypeRead,
			},
			Time: int32(time.Now().UnixNano()),
		}
	}

	var intra bool
	if r < cfg.ReadOnly+cfg.IntraShard {
		intra = true
	} else {
		intra = false
	}

	sender := pickAccount(cfg.Skew)
	senderShard := GetShard(int32(sender))

	var receiver int
	if intra {
		start, end := shardRange(senderShard)
		for {
			receiver = rand.Intn(end-start+1) + start
			if receiver != sender {
				break
			}
		}
	} else {
		var otherShard int
		for {
			c := rand.Intn(3) + 1
			if c != senderShard {
				otherShard = c
				break
			}
		}
		start, end := shardRange(otherShard)
		for {
			receiver = rand.Intn(end-start+1) + start
			if receiver != sender {
				break
			}
		}
	}

	amt := int32(rand.Intn(7))
	globalTime++

	return &client.Txn{
		Sender:   strconv.Itoa(sender),
		Reciever: strconv.Itoa(receiver),
		Amount:   amt,
		Command: client.Command{
			Type: client.CommandTypeTransfer,
		},
		Time: globalTime,
	}
}
