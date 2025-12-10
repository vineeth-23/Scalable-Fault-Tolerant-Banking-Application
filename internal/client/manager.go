package client

import (
	"context"
	"log"
	"strconv"
	"sync"
	"time"

	pb "bank-application/pb/bank-application/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ClientManager orchestrates all clients
type ClientManager struct {
	Clients        map[string]*Client
	clusterLeaders map[int]string
	mu             sync.RWMutex
	peers          map[int32]string

	globalTime int32 // monotonically increasing across all sets

	retryQueue    []*Txn
	retryMu       sync.Mutex
	maxRetryCount int

	AllExecutedTransferTransactions []*Txn
}

// ProcessRetryQueue retries all previously skipped transactions after a RECOVER.
// Successful ones are removed; failed ones are re-queued for future recovery.
func (cm *ClientManager) ProcessRetryQueue() {
	pending := cm.DrainRetryQueue()
	if len(pending) == 0 {
		return
	}

	var wg sync.WaitGroup
	for _, tx := range pending {
		tx := tx
		wg.Add(1)
		go func() {
			defer wg.Done()
			//start := time.Now()
			ok := false
			switch tx.Command.Type {
			case CommandTypeTransfer:
				client, okc := cm.Clients[tx.Sender]
				if !okc {
					return
				}
				ok = client.SendTransaction(tx, cm.peers)
			case CommandTypeRead:
				client, okc := cm.Clients[tx.Sender]
				if !okc {
					return
				}
				ok = client.SendRead(tx, cm.peers)
			default:
				return
			}
			//RecordPerf(ok, time.Since(start))
			if !ok {
				cm.AddToRetryQueue(tx)
			}
		}()
	}
	wg.Wait()
}

func NewClientManager(peers map[int32]string) *ClientManager {
	cm := &ClientManager{
		Clients:       make(map[string]*Client, 9000),
		peers:         peers,
		globalTime:    0,
		retryQueue:    []*Txn{},
		maxRetryCount: 2,
		clusterLeaders: map[int]string{
			1: peers[1],
			2: peers[4],
			3: peers[7],
		},
		AllExecutedTransferTransactions: []*Txn{},
	}

	for i := 1; i <= 9000; i++ {
		clientID := strconv.Itoa(i)

		clusterID := (i-1)/3000 + 1
		leaderAddr := cm.clusterLeaders[clusterID]

		c := &Client{
			ID:              clientID,
			lastKnownLeader: leaderAddr,
			timeout:         15 * time.Second,
			doneCh:          make(chan struct{}, 1),
			manager:         cm,
		}
		c.doneCh <- struct{}{}

		cm.Clients[clientID] = c
	}

	return cm
}

func (cm *ClientManager) Reset() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.globalTime = 0

	cm.retryQueue = []*Txn{}

	cm.Clients = make(map[string]*Client, 9000)

	cm.AllExecutedTransferTransactions = []*Txn{}

	for i := 1; i <= 9000; i++ {
		clientID := strconv.Itoa(i)

		clusterID := (i-1)/3000 + 1
		leaderAddr := cm.clusterLeaders[clusterID]

		c := &Client{
			ID:              clientID,
			lastKnownLeader: leaderAddr,
			timeout:         15 * time.Second,
			doneCh:          make(chan struct{}, 1),
			manager:         cm,
		}
		c.doneCh <- struct{}{}

		cm.Clients[clientID] = c
	}
}

func (cm *ClientManager) SetClusterLeader(nodeID int32) {
	clusterID := GetClusterForNode(nodeID)
	if clusterID == 0 {
		return
	}

	addr, ok := cm.peers[nodeID]
	if !ok {
		return
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.clusterLeaders[clusterID] = addr
}

func (cm *ClientManager) GetClusterLeader(clusterID int) (string, bool) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	addr, ok := cm.clusterLeaders[clusterID]
	return addr, ok
}

// nextGlobalTime atomically increments global time
func (cm *ClientManager) nextGlobalTime() int32 {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.globalTime++
	return cm.globalTime
}

func (cm *ClientManager) AddToRetryQueue(tx *Txn) {
	cm.retryMu.Lock()
	defer cm.retryMu.Unlock()
	cm.retryQueue = append(cm.retryQueue, tx)
}

func (cm *ClientManager) DrainRetryQueue() []*Txn {
	cm.retryMu.Lock()
	defer cm.retryMu.Unlock()
	out := cm.retryQueue
	cm.retryQueue = []*Txn{}
	return out
}

// RunSet processes one set of transactions in parallel per client
//func (cm *ClientManager) RunSet(txns []*Txn) {
//	var wg sync.WaitGroup
//	for _, t := range txns {
//		t.Time = cm.nextGlobalTime()
//		wg.Add(1)
//		go func(tx *Txn) {
//			defer wg.Done()
//			cm.clients[tx.Sender].SendTransaction(tx, cm.peers)
//		}(t)
//	}
//	wg.Wait()
//}

// runSetWithLF processes transactions in batches split by LF commands.
//func (cm *ClientManager) RunSetWithLF(txns []*Txn) {
//	var batch []*Txn
//
//	retryTxns := cm.DrainRetryQueue()
//	if len(retryTxns) > 0 {
//		//log.Printf("Adding %d retried transactions to this set", len(retryTxns))
//		txns = append(retryTxns, txns...)
//	}
//
//	for _, txn := range txns {
//		if txn.Command == "LF" {
//			if len(batch) > 0 {
//				cm.RunSet(batch)
//				batch = nil
//			}
//
//			cm.failLeader()
//
//			time.Sleep(3 * time.Second)
//		} else {
//			batch = append(batch, txn)
//		}
//	}
//
//	// Run last batch
//	if len(batch) > 0 {
//		cm.RunSet(batch)
//	}
//}

func (cm *ClientManager) RunSet(txns []*Txn) {
	i := 0
	n := len(txns)

	for i < n {
		var wg sync.WaitGroup

		for i < n && txns[i].Command.Type != CommandTypeFail && txns[i].Command.Type != CommandTypeRecover {
			tx := txns[i]
			wg.Add(1)

			go func(tx *Txn) {
				defer wg.Done()

				var okExec bool

				switch tx.Command.Type {
				case CommandTypeTransfer:
					client, ok := cm.Clients[tx.Sender]
					if !ok {
						return
					}
					okExec = client.SendTransaction(tx, cm.peers)

				case CommandTypeRead:
					client, ok := cm.Clients[tx.Sender]
					if !ok {
						return
					}
					okExec = client.SendRead(tx, cm.peers)

				default:
					log.Printf("[RunSet] Unexpected non-F/R command type in concurrent block: %+v", tx.Command.Type)
					return
				}

				if !okExec {
					cm.AddToRetryQueue(tx)
				}
			}(tx)

			i++
		}

		wg.Wait()

		if i < n && (txns[i].Command.Type == CommandTypeFail || txns[i].Command.Type == CommandTypeRecover) {
			tx := txns[i]
			cm.UpdateNodeStatus(tx)
			if tx.Command.Type == CommandTypeRecover {
				cm.ProcessRetryQueue()
			}
			i++
		}
	}
}

//func (cm *ClientManager) RunSet(txns []*Txn) {
//	//var wg sync.WaitGroup
//
//	for _, tx := range txns {
//		//wg.Add(1)
//
//		//go func(tx *Txn) {
//		//	defer wg.Done()
//
//		switch tx.Command.Type {
//		case CommandTypeTransfer:
//			client, ok := cm.clients[tx.Sender]
//			if !ok {
//				return
//			}
//			client.SendTransaction(tx, cm.peers)
//
//		case CommandTypeRead:
//			client, ok := cm.clients[tx.Sender]
//			if !ok {
//				return
//			}
//			client.SendRead(tx, cm.peers)
//
//		case CommandTypeFail, CommandTypeRecover:
//			cm.UpdateNodeStatus(tx)
//
//		default:
//			log.Printf("!!Came into Default Mode. Should Not happen!!")
//		}
//		//}(tx)
//	}
//
//	//wg.Wait()
//}

//func (cm *ClientManager) UpdateNodeStatus(tx *Txn) {
//	nodeID := tx.Command.NodeID
//
//	addr, ok := cm.peers[nodeID]
//	if !ok {
//		log.Printf("[UpdateNodeStatus] unknown nodeID %d in command %v", nodeID, tx.Command.Type)
//		return
//	}
//
//	alive := tx.Command.Type == CommandTypeRecover
//
//	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
//	defer cancel()
//
//	conn, err := grpc.NewClient(
//		addr,
//		grpc.WithTransportCredentials(insecure.NewCredentials()),
//	)
//	if err != nil {
//		log.Printf("[UpdateNodeStatus] failed to connect to node %d (%s): %v", nodeID, addr, err)
//		return
//	}
//	defer conn.Close()
//
//	node := pb.NewBankApplicationClient(conn)
//
//	req := &pb.AliveRequest{
//		Alive: alive,
//	}
//
//	resp, err := node.UpdateNodeStatus(ctx, req)
//	if err != nil {
//		log.Printf("[UpdateNodeStatus] RPC to node %d (%s) failed: %v", nodeID, addr, err)
//		return
//	}
//
//	if resp != nil && resp.Success {
//		action := "FAIL"
//		if alive {
//			action = "RECOVER"
//		}
//		log.Printf("[UpdateNodeStatus] node %d updated successfully (%s)", nodeID, action)
//	} else {
//		log.Printf("[UpdateNodeStatus] node %d did not acknowledge status update", nodeID)
//	}
//}

func (cm *ClientManager) UpdateNodeStatus(tx *Txn) {
	failedOrRecoveredNodeID := tx.Command.NodeID
	alive := tx.Command.Type == CommandTypeRecover

	var wg sync.WaitGroup

	for peerID, addr := range cm.peers {
		wg.Add(1)

		go func(peerID int32, addr string) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			conn, err := grpc.NewClient(
				addr,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			if err != nil {
				if peerID == failedOrRecoveredNodeID {
					log.Printf("[UpdateNodeStatus] failed to connect to node %d (%s): %v",
						peerID, addr, err)
				}
				return
			}
			defer conn.Close()

			node := pb.NewBankApplicationClient(conn)

			onlyUpdatePeersAndAliveList := peerID != failedOrRecoveredNodeID

			req := &pb.AliveRequest{
				Alive:                                 alive,
				OnlyUpdateAliveNodesAndAlivePeersList: onlyUpdatePeersAndAliveList,
				OnlyUpdateAliveNodesAndPeersList: &pb.OnlyUpdateAliveNodesAndPeersList{
					UpdatedStatusNodeId: failedOrRecoveredNodeID,
					Alive:               alive,
				},
			}

			resp, err := node.UpdateNodeStatus(ctx, req)
			if err != nil {
				if peerID == failedOrRecoveredNodeID {
					log.Printf("[UpdateNodeStatus] RPC to node %d (%s) failed: %v",
						peerID, addr, err)
				}
				return
			}

			if peerID == failedOrRecoveredNodeID {
				if resp != nil && resp.Success {
					action := "FAIL"
					if alive {
						action = "RECOVER"
					}
					log.Printf("[UpdateNodeStatus] node %d updated successfully (%s)", peerID, action)
				} else {
					log.Printf("[UpdateNodeStatus] node %d did not acknowledge status update", peerID)
				}
			}

		}(peerID, addr)
	}

	wg.Wait()
}
