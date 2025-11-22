package client

import (
	"log"
	"strconv"
	"strings"
	"time"

	pb "bank-application/pb/bank-application/pb"
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

func (c *Client) SendTransaction(tx *Txn, allPeers map[int32]string) {
	<-c.doneCh
	defer func() { c.doneCh <- struct{}{} }()

	clientID, _ := strconv.Atoi(tx.Sender)
	clusterID := GetClusterIDForClient(int32(clientID))
	clusterPeers := GetClusterForClient(int32(clientID))
	rootNode := GetClusterRootNode(int32(clientID))
	rootAddr := allPeers[int32(rootNode)]

	for attempt := 1; attempt <= maxRetries; attempt++ {
		if leaderAddr, ok := c.manager.GetClusterLeader(clusterID); ok && leaderAddr != "" {
			ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
			ok := c.handleClientRequest(ctx, leaderAddr, tx)
			cancel()

			if ok {
				log.Printf("[Client %s] Txn: (%s->%s (%d) time: %d) executed via cluster leader %s (attempt %d)",
					c.ID, tx.Sender, tx.Reciever, tx.Amount, tx.Time, leaderAddr, attempt)
				return
			}
		}

		{
			ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
			ok := c.handleClientRequest(ctx, rootAddr, tx)
			cancel()

			if ok {
				log.Printf("[Client %s] Txn: (%s->%s (%d) time: %d) executed via rootNode %s (attempt %d)",
					c.ID, tx.Sender, tx.Reciever, tx.Amount, tx.Time, rootAddr, attempt)
				return
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
						cancel2()
					default:
					}
				}
			}(addr)
		}

		select {
		case <-successCh:
			cancel2()
			return

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
}

func (c *Client) SendRead(tx *Txn, allPeers map[int32]string) {
	<-c.doneCh
	defer func() { c.doneCh <- struct{}{} }()

	clientID, _ := strconv.Atoi(tx.Sender)
	clusterID := GetClusterIDForClient(int32(clientID))

	if leaderAddr, ok := c.manager.GetClusterLeader(clusterID); ok && leaderAddr != "" {
		ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
		ok := c.handleReadRPC(ctx, leaderAddr, tx)
		cancel()
		if ok {
			return
		}
	}

	clusterPeers := GetClusterForClient(int32(clientID))
	rootNode := GetClusterRootNode(int32(clientID))

	rootAddr := allPeers[int32(rootNode)]
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	ok := c.handleReadRPC(ctx, rootAddr, tx)
	cancel()
	if ok {
		return
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
