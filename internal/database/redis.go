package database

import (
	"bank-application/internal/common"
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

var (
	rdb *redis.Client
	ctx = context.Background()
)

func InitRedisClient(addr string) {
	rdb = redis.NewClient(&redis.Options{
		Addr: addr,
		DB:   0,
	})
}

func DeleteClientBalance(nodeID int32, clientID string) error {
	key := fmt.Sprintf("node:%d", nodeID)
	return rdb.HDel(ctx, key, clientID).Err()
}

func ClusterNodeIDs(cluster int) []int32 {
	switch cluster {
	case 1:
		return []int32{1, 2, 3}
	case 2:
		return []int32{4, 5, 6}
	case 3:
		return []int32{7, 8, 9}
	default:
		return []int32{}
	}
}

func GetClusterBalance(cluster int, clientID int) (int32, error) {
	nodes := ClusterNodeIDs(cluster)
	if len(nodes) == 0 {
		return 0, fmt.Errorf("invalid cluster %d", cluster)
	}
	cid := strconv.Itoa(clientID)
	for _, nid := range nodes {
		if bal, err := GetClientBalance(nid, cid); err == nil {
			return bal, nil
		}
	}
	return 0, fmt.Errorf("balance for client %d not found in cluster %d", clientID, cluster)
}

func SetClusterBalance(cluster int, clientID int, balance int32) error {
	nodes := ClusterNodeIDs(cluster)
	if len(nodes) == 0 {
		return fmt.Errorf("invalid cluster %d", cluster)
	}
	cid := strconv.Itoa(clientID)
	for _, nid := range nodes {
		if err := UpdateClientBalance(nid, cid, balance); err != nil {
			return err
		}
	}
	return nil
}

func DeleteClusterBalance(cluster int, clientID int) error {
	nodes := ClusterNodeIDs(cluster)
	if len(nodes) == 0 {
		return fmt.Errorf("invalid cluster %d", cluster)
	}
	cid := strconv.Itoa(clientID)
	for _, nid := range nodes {
		if err := DeleteClientBalance(nid, cid); err != nil {
			return err
		}
	}
	return nil
}

func UpdateClientBalance(nodeID int32, clientID string, balance int32) error {
	key := fmt.Sprintf("node:%d", nodeID)
	return rdb.HSet(ctx, key, clientID, balance).Err()
}

func GetClientBalance(nodeID int32, clientID string) (int32, error) {
	key := fmt.Sprintf("node:%d", nodeID)
	val, err := rdb.HGet(ctx, key, clientID).Int()
	return int32(val), err
}

func GetAllClientBalances(nodeID int32) (map[string]int32, error) {
	key := fmt.Sprintf("node:%d", nodeID)
	res := make(map[string]int32)
	vals, err := rdb.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	for k, v := range vals {
		var intval int
		fmt.Sscanf(v, "%d", &intval)
		res[k] = int32(intval)
	}
	return res, nil
}

func AddWALEntry(nodeID int32, entry *common.WALEntry) error {
	key := fmt.Sprintf("wal:%d", nodeID)

	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	return rdb.HSet(ctx, key, fmt.Sprintf("%d", entry.TxnTime), data).Err()
}

func DeleteWALEntry(nodeID int32, txnTime int32) error {
	key := fmt.Sprintf("wal:%d", nodeID)
	return rdb.HDel(ctx, key, fmt.Sprintf("%d", txnTime)).Err()
}

const shardMapKey = "shardmap"

func EnsureShardMapInitialized(totalAccounts int) error {
	n, err := rdb.HLen(ctx, shardMapKey).Result()
	if err != nil {
		return err
	}
	if n > 0 {
		return nil
	}

	fields := make([]interface{}, 0, totalAccounts*2)
	for acc := 1; acc <= totalAccounts; acc++ {
		c := common.GetClusterIDForClient(int32(acc))
		fields = append(fields, strconv.Itoa(acc))
		fields = append(fields, strconv.Itoa(c))
	}
	if len(fields) == 0 {
		return nil
	}
	return rdb.HSet(ctx, shardMapKey, fields...).Err()
}

func IntializeShardMap(totalAccounts int) error {
	fields := make([]interface{}, 0, totalAccounts*2)
	for acc := 1; acc <= totalAccounts; acc++ {
		c := common.GetClusterIDForClient(int32(acc))
		fields = append(fields, strconv.Itoa(acc))
		fields = append(fields, strconv.Itoa(c))
	}
	if len(fields) == 0 {
		return nil
	}
	return rdb.HSet(ctx, shardMapKey, fields...).Err()
}

func GetShardMapping(clientID int) (int, error) {
	if clientID <= 0 {
		return 0, nil
	}
	s, err := rdb.HGet(ctx, shardMapKey, strconv.Itoa(clientID)).Result()
	if err == redis.Nil {
		// fallback to default range mapping
		return common.GetClusterIDForClient(int32(clientID)), nil
	}
	if err != nil {
		return 0, err
	}
	v, convErr := strconv.Atoi(s)
	if convErr != nil {
		return 0, convErr
	}
	if v < 1 || v > 3 {
		// sanitize
		return common.GetClusterIDForClient(int32(clientID)), nil
	}
	return v, nil
}

func BulkSetShardMappings(m map[int]int) error {
	if len(m) == 0 {
		return nil
	}
	fields := make([]interface{}, 0, len(m)*2)
	for acc, c := range m {
		if acc <= 0 || c < 1 || c > 3 {
			continue
		}
		fields = append(fields, strconv.Itoa(acc))
		fields = append(fields, strconv.Itoa(c))
	}
	if len(fields) == 0 {
		return nil
	}
	return rdb.HSet(ctx, shardMapKey, fields...).Err()
}

func GetAllShardMappings() (map[int]int, error) {
	vals, err := rdb.HGetAll(ctx, shardMapKey).Result()
	if err != nil {
		return nil, err
	}
	out := make(map[int]int, len(vals))
	for k, v := range vals {
		ki, err1 := strconv.Atoi(k)
		vi, err2 := strconv.Atoi(v)
		if err1 != nil || err2 != nil {
			continue
		}
		out[ki] = vi
	}
	return out, nil
}
