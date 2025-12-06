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

// ---------------------- Balance migration helpers ----------------------

// DeleteClientBalance removes a client's balance entry from a specific node's bucket.
func DeleteClientBalance(nodeID int32, clientID string) error {
	key := fmt.Sprintf("node:%d", nodeID)
	return rdb.HDel(ctx, key, clientID).Err()
}

// ClusterNodeIDs returns the three node IDs that belong to a cluster.
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

// GetClusterBalance tries to read a client's balance from any node within the given cluster.
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

// SetClusterBalance writes a client's balance to all nodes in a cluster.
func SetClusterBalance(cluster int, clientID int, balance int32) error {
	nodes := ClusterNodeIDs(cluster)
	if len(nodes) == 0 {
		return fmt.Errorf("invalid cluster %d", cluster)
	}
	cid := strconv.Itoa(clientID)
	for _, nid := range nodes {
		//log.Printf("Updating client balance for client id: %d to %d at node-%d", clientID, balance, nid)
		if err := UpdateClientBalance(nid, cid, balance); err != nil {
			return err
		}
	}
	return nil
}

// DeleteClusterBalance removes a client's balance from all nodes in a cluster.
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

// ---------------------- Shard Mapping (client -> cluster) ----------------------

const shardMapKey = "shardmap"

// EnsureShardMapInitialized initializes the shard map with default range-based
// mapping if it is currently empty. Subsequent calls are no-ops.
func EnsureShardMapInitialized(totalAccounts int) error {
	// If the hash already has fields, do nothing
	n, err := rdb.HLen(ctx, shardMapKey).Result()
	if err != nil {
		return err
	}
	if n > 0 {
		return nil
	}

	// Initialize defaults according to range-based sharding
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
	// Initialize defaults according to range-based sharding
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

// GetShardMapping returns the cluster ID [1..3] for a given client.
// Falls back to range-based mapping if none is stored.
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

// BulkSetShardMappings writes multiple client->cluster assignments atomically.
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

// GetAllShardMappings returns all client->cluster assignments currently stored.
// If the map is empty, the caller may assume default range mapping applies.
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
