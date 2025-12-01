package common

import "strconv"

var Peers = map[int32]string{
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

const (
	SenderNotValid   = "SENDER_NOT_VALID"
	ReceiverNotValid = "RECEIVER_NOT_VALID"
)

type ShardType string

const (
	ShardTypeIntra ShardType = "INTRA_SHARD"
	ShardTypeCross ShardType = "CROSS_SHARD"
)

type PhaseType string

const (
	PhasePrepare PhaseType = "PREPARE"
	PhaseCommit  PhaseType = "COMMIT"
	PhaseAbort   PhaseType = "ABORT"
)

type AdditionalParameteres struct {
	Shard ShardType
	Phase PhaseType
}

type WALEntry struct {
	TxnTime  int32
	ClientID string
	OldValue int32
	NewValue int32
}

func ClusterOf(nodeID int32) int {
	switch {
	case nodeID >= 1 && nodeID <= 3:
		return 1
	case nodeID >= 4 && nodeID <= 6:
		return 2
	case nodeID >= 7 && nodeID <= 9:
		return 3
	default:
		return -1
	}
}

func IsIntraShard(sender, receiver string, nodeID int32) bool {
	senderID, err1 := strconv.Atoi(sender)
	receiverID, err2 := strconv.Atoi(receiver)
	if err1 != nil || err2 != nil {
		return true
	}

	start, end := ShardRangeForNode(nodeID)

	sID := int32(senderID)
	rID := int32(receiverID)

	return sID >= start && sID <= end && rID >= start && rID <= end
}

func ShardRangeForNode(id int32) (start, end int32) {
	c := ClusterOf(id)
	start = int32((c-1)*3000 + 1)
	end = int32(c * 3000)
	return
}

func GetClusterIDForClient(clientID int32) int {
	switch {
	case clientID >= 1 && clientID <= 3000:
		return 1
	case clientID >= 3001 && clientID <= 6000:
		return 2
	case clientID >= 6001 && clientID <= 9000:
		return 3
	default:
		return 0
	}
}

func GetClusterForClient(clientID int32) []int32 {
	switch {
	case clientID >= 1 && clientID <= 3000:
		return []int32{1, 2, 3}
	case clientID >= 3001 && clientID <= 6000:
		return []int32{4, 5, 6}
	case clientID >= 6001 && clientID <= 9000:
		return []int32{7, 8, 9}
	default:
		return []int32{}
	}
}
