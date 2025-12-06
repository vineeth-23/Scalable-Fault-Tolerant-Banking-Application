package client

import (
	"log"
	"strconv"
	"strings"
	"time"

	pb "bank-application/pb/bank-application/pb"
	"bank-application/internal/database"
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	ID              string
	lastKnownLeader string
	timeout         time.Duration
	doneCh          chan struct{}
	manager         *ClientManager
}

const (
	maxRetries = 4
)

// dynamicClusterID returns the cluster ID for a client using the Redis-backed
// shard mapping. Falls back to the default range-based mapping if not present.
func dynamicClusterID(clientID int32) int {
    c, err := database.GetShardMapping(int(clientID))
    if err == nil && c >= 1 && c <= 3 {
        return c
    }
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

func clusterPeersForCluster(c int) []int32 {
    switch c {
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

func clusterRootNodeForCluster(c int) int32 {
    switch c {
    case 1:
        return 1
    case 2:
        return 4
    case 3:
        return 7
    default:
        return -1
    }
}

func (c *Client) SendTransaction(tx *Txn, allPeers map[int32]string) bool {
	<-c.doneCh
	defer func() { c.doneCh <- struct{}{} }()

	clientID, _ := strconv.Atoi(tx.Sender)
	clusterID := dynamicClusterID(int32(clientID))
	clusterPeers := clusterPeersForCluster(clusterID)
	rootNode := clusterRootNodeForCluster(clusterID)
	rootAddr := allPeers[int32(rootNode)]

	for attempt := 1; attempt <= maxRetries; attempt++ {
		if leaderAddr, ok := c.manager.GetClusterLeader(clusterID); ok && leaderAddr != "" {
			ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
			ok := c.handleClientRequest(ctx, leaderAddr, tx)
			cancel()

			if ok {
				log.Printf("[Client %s] Txn: (%s->%s (%d) time: %d) executed via cluster leader %s (attempt %d)",
					c.ID, tx.Sender, tx.Reciever, tx.Amount, tx.Time, leaderAddr, attempt)
				//log.Printf("Length of AllExecutedTransferTransactions is %d", len(c.manager.AllExecutedTransferTransactions))
				c.manager.AllExecutedTransferTransactions = append(c.manager.AllExecutedTransferTransactions, tx)
				return true
			}
		}

		{
			ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
			ok := c.handleClientRequest(ctx, rootAddr, tx)
			cancel()

			if ok {
				log.Printf("[Client %s] Txn: (%s->%s (%d) time: %d) executed via rootNode %s (attempt %d)",
					c.ID, tx.Sender, tx.Reciever, tx.Amount, tx.Time, rootAddr, attempt)
				c.manager.AllExecutedTransferTransactions = append(c.manager.AllExecutedTransferTransactions, tx)
				return true
			}
		}

		ctx2, cancel2 := context.WithTimeout(context.Background(), c.timeout)
		successCh := make(chan struct{}, 1)

		for _, nid := range clusterPeers {
			addr := allPeers[nid]

			go func(addr string) {
				if c.handleClientRequest(ctx2, addr, tx) {
					select {
					case successCh <- struct{}{}:
						log.Printf("[Client %s] Txn: (%s->%s (%d) time: %d) executed via multicast at %s (attempt %d)",
							c.ID, tx.Sender, tx.Reciever, tx.Amount, tx.Time, addr, attempt)
						c.manager.AllExecutedTransferTransactions = append(c.manager.AllExecutedTransferTransactions, tx)
						cancel2()
					default:
					}
				}
			}(addr)
		}

		select {
		case <-successCh:
			cancel2()
			return true

		case <-ctx2.Done():
			cancel2()
			if attempt < maxRetries {
				continue
			}
		}

		break
	}

	log.Printf("[Client %s] Txn: (%s->%s (%d) time: %d) TIMED OUT (no peer in cluster responded after %d attempts)",
		c.ID, tx.Sender, tx.Reciever, tx.Amount, tx.Time, 4)
	return false
}

func (c *Client) SendRead(tx *Txn, allPeers map[int32]string) bool {
	<-c.doneCh
	defer func() { c.doneCh <- struct{}{} }()

	clientID, _ := strconv.Atoi(tx.Sender)
	clusterID := dynamicClusterID(int32(clientID))

	if leaderAddr, ok := c.manager.GetClusterLeader(clusterID); ok && leaderAddr != "" {
		ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
		ok := c.handleReadRPC(ctx, leaderAddr, tx)
		cancel()
		if ok {
			return true
		}
	}

	clusterPeers := clusterPeersForCluster(clusterID)
	rootNode := clusterRootNodeForCluster(clusterID)

	rootAddr := allPeers[int32(rootNode)]
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	ok := c.handleReadRPC(ctx, rootAddr, tx)
	cancel()
	if ok {
		return true
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), c.timeout)
	respCh := make(chan bool, len(clusterPeers))

	for _, nid := range clusterPeers {
		addr := allPeers[nid]
		go func(addr string) {
			ok := c.handleReadRPC(ctx2, addr, tx)
			if ok {
				select {
				case respCh <- true:
				default:
				}
			}
		}(addr)
	}

	select {
	case <-respCh:
		// success
	case <-ctx2.Done():
		// failed
	}
	cancel2()
	return false
}

func (c *Client) handleClientRequest(ctx context.Context, addr string, tx *Txn) bool {
	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Printf("[Client %s] failed to connect to %s: %v", c.ID, addr, err)
		return false
	}
	defer conn.Close()

	node := pb.NewBankApplicationClient(conn)

	req := &pb.ClientRequestMessage{
		MessageType: pb.MessageType_REQUEST,
		Time:        tx.Time,
		Transaction: &pb.Transaction{
			Sender:   tx.Sender,
			Reciever: tx.Reciever,
			Amount:   tx.Amount,
		},
		ClientNumber: tx.Sender,
	}

	resp, err := node.HandleClientRequest(ctx, req)
	if err != nil {
		if strings.Contains(err.Error(), "leader election in progress") {
			log.Printf("[Client %s] leader election in progress at %s for tx %+v: %v",
				c.ID, addr, tx, err)
			return false
		}
		//log.Printf("[Client %s] HandleClientRequest to %s failed for tx %+v: %v",
		//	c.ID, addr, tx, err)
		return false
	}

	if resp != nil && resp.BallotNo != nil {
		leaderID := resp.BallotNo.NodeNo
		clusterID := GetClusterForNode(leaderID)

		if clusterID == 0 {
			log.Printf("[Client %s] received BallotNo with unknown cluster node %d from %s",
				c.ID, leaderID, addr)
			return true
		}

		if leaderAddr, ok := c.manager.peers[leaderID]; ok {
			prev, _ := c.manager.GetClusterLeader(clusterID)
			if prev != leaderAddr {
				log.Printf("[Client %s] updating leader for cluster %d to node %d",
					c.ID, clusterID, leaderID)
			}
			c.manager.SetClusterLeader(leaderID)
		} else {
			log.Printf("[Client %s] received BallotNo with unknown node %d from %s",
				c.ID, leaderID, addr)
		}
	}

	return true
}

func (c *Client) handleReadRPC(ctx context.Context, addr string, tx *Txn) bool {

	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Printf("[Client %s] READ connect to %s failed: %v", c.ID, addr, err)
		return false
	}
	defer conn.Close()

	node := pb.NewBankApplicationClient(conn)

	req := &pb.ReadClientBalanceRequest{
		Time:     tx.Time,
		ClientId: tx.Sender,
	}

	resp, err := node.ReadClientBalance(ctx, req)
	if err != nil {
		log.Printf("[Client %s] READ RPC to %s failed: %v", c.ID, addr, err)
		return false
	}

	log.Printf("[Client %s] READ response: client=%s balance=%d from %s",
		c.ID, tx.Sender, resp.Balance, addr)

	return true
}
