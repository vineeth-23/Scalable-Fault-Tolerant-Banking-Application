package client

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

func GetClusterRootNode(clientID int32) int32 {
	switch {
	case clientID >= 1 && clientID <= 3000:
		return 1
	case clientID >= 3001 && clientID <= 6000:
		return 4
	case clientID >= 6001 && clientID <= 9000:
		return 7
	default:
		return -1
	}
}

func GetClusterForNode(nodeID int32) int {
	switch {
	case nodeID >= 1 && nodeID <= 3:
		return 1
	case nodeID >= 4 && nodeID <= 6:
		return 2
	case nodeID >= 7 && nodeID <= 9:
		return 3
	default:
		return 0
	}
}
