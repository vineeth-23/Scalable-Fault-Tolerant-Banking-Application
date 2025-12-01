package node

import (
	"bank-application/internal/common"
	"bank-application/internal/database"
	"context"
	"log"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"

	pb "bank-application/pb/bank-application/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type BallotNumber struct {
	TermNo int32
	NodeNo int32
}

// LogEntry keeps track of transactions and their status
type LogEntry struct {
	BallotNumber   *BallotNumber
	SequenceNumber int64
	Request        *pb.ClientRequestMessage
	Status         string
}

//type WALEntry struct {
//	TxnTime  int32
//	ClientID string
//	OldValue int32
//	NewValue int32
//}

// Node represents a Paxos node
type Node struct {
	mu            sync.Mutex
	randMu        sync.Mutex
	lastRepliesMu sync.RWMutex

	rng *rand.Rand

	ID      int32
	Address string

	IsLeader        bool
	CurrentLeaderID int32

	CurrentBallot            *BallotNumber
	SequenceNumber           int64
	SequenceNumberToLogEntry map[int32]*LogEntry

	Balances map[string]int32

	Peers map[int32]string // nodeID -> address (all known peers)

	LastExecutedSequenceNumber int32
	LastReplies                map[string]*pb.ClientResponseMessage

	// Election timeout state
	electionTimer          *time.Timer
	electionTimeoutMin     time.Duration
	electionTimeoutMax     time.Duration
	currentElectionTimeout time.Duration

	// For collecting promises when I am proposer
	promiseMu             sync.Mutex
	promiseInbox          map[int32]*pb.PromiseMessage
	promisesQuorumReached bool

	// For collecting prepare messages when I am follower
	pendingPrepares []*pb.PrepareMessage

	NewViewMessages []*pb.NewViewRequest
	AllMessages     []*LogEntry

	// To handle: A node will only send a prepare message if it has not received any prepare messages in the last tp milliseconds.
	lastPrepareSeen time.Time
	tpDuration      time.Duration

	lastElectionStarted time.Time

	isAlive    bool
	AliveNodes []int32

	heartbeatTicker *time.Ticker
	heartbeatStopCh chan struct{}

	replyCond *sync.Cond

	// fields added for 2PC impltn
	LockTable         map[string]bool
	twoPCTimeout      time.Duration
	AliveClusterPeers map[int32]string // nodeID -> address (all peers which belongs to same cluster)
	WAL               map[int32]*common.WALEntry
}

func NewNode(id int32, address string, peers map[int32]string) *Node {
	balances := make(map[string]int32, 3000)
	start, end := common.ShardRangeForNode(id)
	for acc := start; acc <= end; acc++ {
		key := strconv.Itoa(int(acc))
		balances[key] = 10
	}
	for acc := start; acc <= end; acc++ {
		clientID := strconv.Itoa(int(acc))
		err := database.UpdateClientBalance(id, clientID, 10)
		if err != nil {
			log.Printf("[Node %d] Failed to reset Redis balance for node=%d client=%s: %v",
				id, id, clientID, err)
		}
	}

	node := &Node{
		ID:       id,
		Address:  address,
		IsLeader: false,

		CurrentBallot: &BallotNumber{
			NodeNo: id,
		},
		SequenceNumber:           0,
		SequenceNumberToLogEntry: make(map[int32]*LogEntry),
		Balances:                 balances,

		Peers:       peers,
		LastReplies: make(map[string]*pb.ClientResponseMessage),

		promiseInbox: make(map[int32]*pb.PromiseMessage),
		isAlive:      true,
		LockTable:    make(map[string]bool),
		twoPCTimeout: 5 * time.Second,
		WAL:          make(map[int32]*common.WALEntry),
	}

	seed := time.Now().UnixNano() + int64(id)*1000003
	node.rng = rand.New(rand.NewSource(seed))
	node.NewViewMessages = []*pb.NewViewRequest{}
	node.AllMessages = []*LogEntry{}

	node.electionTimeoutMin = 1000 * time.Millisecond
	node.electionTimeoutMax = 2000 * time.Millisecond
	node.tpDuration = 500 * time.Millisecond

	node.lastPrepareSeen = time.Now()
	//node.replyCond = sync.NewCond(&node.mu)

	myCluster := common.ClusterOf(id)
	node.AliveClusterPeers = make(map[int32]string)

	for peerID, addr := range peers {
		if common.ClusterOf(peerID) == myCluster {
			node.AliveClusterPeers[peerID] = addr
		}
	}

	log.Printf("Intialised node-%d", id)

	//initialDelay := time.Duration(rand.Int63n(int64(node.electionTimeoutMin)))
	//log.Printf("[Node %d] will schedule election after initial delay %v", node.ID, initialDelay)

	//time.AfterFunc(initialDelay, func() {
	//node.ScheduleNextElection()
	//})

	//node.ScheduleNextElection()

	return node
}

func (n *Node) randInt63n(max int64) int64 {
	if max <= 0 {
		return 0
	}

	n.randMu.Lock()
	defer n.randMu.Unlock()

	if n.rng == nil {
		seed := time.Now().UnixNano() + int64(n.ID)*1000003
		n.rng = rand.New(rand.NewSource(seed))
	}

	return n.rng.Int63n(max)
}

func (n *Node) randDuration(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}

	return time.Duration(n.randInt63n(int64(max)))
}

// StartServer boots up the gRPC server on node.Address
func (s *NodeServer) StartServer() {
	addr := s.node.Address
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Node %s failed to listen on %s: %v", s.node.ID, addr, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterBankApplicationServer(grpcServer, s)

	log.Printf("Node %d running gRPC server at %s", s.node.ID, addr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Node %s failed to serve: %v", s.node.ID, err)
	}
}

func pickHighestBallot(bList []*pb.PrepareMessage) *pb.BallotNumber {
	if len(bList) == 0 {
		return nil
	}
	var best *pb.BallotNumber
	for _, p := range bList {
		if best == nil || isHigherBallot(p.GetBallotNumber(), best) {
			best = p.GetBallotNumber()
		}
	}
	return best
}

// Accept a prepare and respond with a promise
func (n *Node) acceptPrepare(ballot *pb.BallotNumber) {
	// update my highest ballot
	n.CurrentBallot = &BallotNumber{
		TermNo: ballot.TermNo,
		NodeNo: ballot.NodeNo,
	}

	// Build promise message with my accepted log
	acceptLog := []*pb.AcceptLogEntry{}
	for _, entry := range n.SequenceNumberToLogEntry {
		if entry.Status == "ACCEPTED" || entry.Status == "COMMITTED" || entry.Status == "EXECUTED" {
			acceptLog = append(acceptLog, &pb.AcceptLogEntry{
				BallotNumber:   &pb.BallotNumber{TermNo: entry.BallotNumber.TermNo, NodeNo: entry.BallotNumber.NodeNo},
				SequenceNumber: int32(entry.SequenceNumber),
				AcceptValue:    entry.Request,
				Status:         entry.Status,
			})
		}
	}

	promise := &pb.PromiseMessage{
		BallotNumber: ballot,
		AcceptLog:    acceptLog,
		FromNodeId:   n.ID,
	}
	n.recordMessageLocked("PROMISE", "SEND", promise.GetBallotNumber(), 0, nil)

	// Send PROMISE to proposer
	proposerAddr := n.Peers[ballot.NodeNo]
	go func() {
		conn, err := grpc.NewClient(
			proposerAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			//log.Printf("[Node %d] Failed to connect to proposer %d: %v", n.ID, ballot.NodeNo, err)
			return
		}
		defer conn.Close()

		node := pb.NewBankApplicationClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		_, err = node.Promise(ctx, promise)
		if err != nil {
			log.Printf("[Node %d] Failed to send PROMISE to proposer %d: %v", n.ID, ballot.NodeNo, err)
		}
		//else {
		//	log.Printf("[Node %d] Sent PROMISE to proposer %d", n.ID, ballot.NodeNo)
		//}
	}()
}

func (n *Node) resetElectionTimerOnLeaderActivity() {
	if n.IsLeader {
		return
	}
	n.ScheduleNextElection()
}

func (n *Node) ScheduleNextElection() {
	if !n.isAlive {
		//log.Printf("[Node %d] is not alive, so cant enter into scheduleNextElection", n.ID)
		return
	}

	// Pick random election timeout
	span := n.electionTimeoutMax - n.electionTimeoutMin

	var randDelay time.Duration
	if span > 0 {
		randDelay = n.randDuration(span)
	}

	timeout := n.electionTimeoutMin + randDelay
	n.currentElectionTimeout = timeout

	// Derive tpDuration ~ 1/2 of election timeout + small random jitter
	base := timeout / 2

	var jitter time.Duration
	if base > 0 {
		jitter = n.randDuration(base / 2)
	}

	n.tpDuration = base + jitter
	if n.tpDuration <= 0 {
		n.tpDuration = base
		if n.tpDuration <= 0 {
			n.tpDuration = timeout / 2
		}
	}

	if n.electionTimer != nil {
		n.electionTimer.Stop()
		n.electionTimer = nil
	}

	//log.Printf("[Node %d] Entered into ScheduleNextElection | timeout=%v | now=%v",
	//	n.ID, timeout, time.Now())

	// Fire after currentElectionTimeout
	n.electionTimer = time.AfterFunc(timeout, func() {
		//log.Printf("Timeout of [Node %d] is completed, entered further to intiate election", n.ID)
		if !n.isAlive {
			//log.Printf("[Node %d] is not alive, so cant start process to get into leaderElection", n.ID)
			return
		}

		var (
			doReschedule    bool
			doStartElection bool
		)

		n.mu.Lock()
		if n.IsLeader {
			//log.Printf("Ideally it should not come because I'm the leader already")
			n.mu.Unlock()
			return
		}

		var highest *pb.BallotNumber
		if len(n.pendingPrepares) > 0 {
			//log.Printf("[Node %d] Length of pending prepares is > 0", n.ID)
			highest = pickHighestBallot(n.pendingPrepares)
		}
		if highest != nil && isHigherBallot(highest, &pb.BallotNumber{
			TermNo: n.CurrentBallot.TermNo,
			NodeNo: n.CurrentBallot.NodeNo,
		}) {
			//log.Printf("Found the higher ballot and sending acceptPrepare")
			n.pendingPrepares = nil
			// updated
			//n.mu.Unlock()
			n.acceptPrepare(highest)
			doReschedule = true
		} else {
			diff := time.Since(n.lastPrepareSeen)
			if diff > n.tpDuration {
				//log.Printf("Call start election as diff > n.tpDuration")
				doStartElection = true
			} else {
				// delay election
				delay := n.tpDuration - diff
				//log.Printf("We are waiting for %v time of delay", delay)
				time.AfterFunc(delay, func() {
					//log.Printf("[Node %d] Starting election after tpDuration delay", n.ID)
					//log.Printf("Schedule Next election: After unlock")
					if !n.IsLeader {
						n.startElection()
					}
				})
			}
		}
		n.mu.Unlock()

		// Perform actions outside the lock
		if doReschedule {
			//log.Printf("[Node %d] Rescheduling next election timer", n.ID)
			n.ScheduleNextElection()
			return
		}
		if doStartElection {
			//log.Printf("[Node %d] Starting election as diff > n.tpDuration", n.ID)
			n.startElection()
		}
	})
}

// startElection is called when this node's election timer expires
func (n *Node) startElection() {
	if !n.isAlive {
		//log.Printf("[Node %d] is not alive, so cant enter into startElection", n.ID)
		return
	}
	//log.Printf("[Node %d] Starting election", n.ID)
	//now := time.Now()
	// Prevent spamming elections: only allow if enough time passed
	//log.Printf("lastElectionStarted=%s, now=%s",
	//	n.lastElectionStarted.Format(time.RFC3339Nano),
	//	now.Format(time.RFC3339Nano))
	//  n.currentElectionTimeout

	//log.Printf("Before lock in Starting election")
	n.mu.Lock()
	// bump term and claim a new ballot
	//log.Printf("[Node %d] Starting election inside", n.ID)
	n.CurrentBallot.TermNo++
	n.CurrentBallot.NodeNo = n.ID
	ballot := *n.CurrentBallot
	n.promisesQuorumReached = false
	n.lastElectionStarted = time.Now()

	//log.Printf("[Node %d] Starting election with ballot=(%d,%d)", n.ID, ballot.TermNo, ballot.NodeNo)

	// reset promise state
	n.promiseMu.Lock()
	n.promiseInbox = make(map[int32]*pb.PromiseMessage) // nodeID -> Promise
	n.promiseMu.Unlock()
	n.mu.Unlock()
	// broadcast PREPARE to all peers
	var wg sync.WaitGroup
	for peerID, addr := range n.AliveClusterPeers {
		if peerID == n.ID {
			continue
		}
		wg.Add(1)
		go func(peerID int32, peerAddr string) {
			defer wg.Done()

			conn, err := grpc.NewClient(peerAddr,
				grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				//log.Printf("[Node %d] Could not connect to peer %d for PREPARE: %v", n.ID, peerID, err)
				return
			}
			defer conn.Close()

			node := pb.NewBankApplicationClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			prepareReq := &pb.PrepareMessage{
				BallotNumber: &pb.BallotNumber{
					TermNo: ballot.TermNo,
					NodeNo: ballot.NodeNo,
				},
			}

			n.recordMessage("PREPARE", "SEND", prepareReq.GetBallotNumber(), 0, nil)

			// fire PREPARE
			_, err = node.Prepare(ctx, prepareReq)
			if err != nil {
				//log.Printf("[Node %d] PREPARE RPC to %d failed: %v", n.ID, peerID, err)
				return
			}
			//log.Printf("[Node %d] Sent PREPARE to node %d", n.ID, peerID)
		}(peerID, addr)
	}
	wg.Wait()
	n.ScheduleNextElection()
}

func (n *Node) majority() int {
	//return (len(n.AliveClusterPeers) / 2) + 1
	return 2
}

// mergePromiseLogs merges promise logs from quorum into a single ordered log.
func (n *Node) mergePromiseLogs() []*pb.AcceptLogEntry {
	n.promiseMu.Lock()
	localPromises := make([]*pb.PromiseMessage, 0, len(n.promiseInbox)+1)

	// pushing my own log into the promise log for merging
	acceptLog := []*pb.AcceptLogEntry{}
	for _, entry := range n.SequenceNumberToLogEntry {
		if entry.Status == "ACCEPTED" || entry.Status == "COMMITTED" || entry.Status == "EXECUTED" {
			acceptLog = append(acceptLog, &pb.AcceptLogEntry{
				BallotNumber:   &pb.BallotNumber{TermNo: entry.BallotNumber.TermNo, NodeNo: entry.BallotNumber.NodeNo},
				SequenceNumber: int32(entry.SequenceNumber),
				AcceptValue:    entry.Request,
				Status:         entry.Status,
			})
		}
	}
	n.promiseInbox[n.ID] = &pb.PromiseMessage{
		BallotNumber: &pb.BallotNumber{
			TermNo: n.CurrentBallot.TermNo,
			NodeNo: n.CurrentBallot.NodeNo,
		},
		AcceptLog:  acceptLog,
		FromNodeId: n.ID,
	}

	for _, p := range n.promiseInbox {
		localPromises = append(localPromises, p)
	}
	n.promiseMu.Unlock()

	merged := make(map[int32]*pb.AcceptLogEntry)
	maxSeq := int32(0)

	for _, promise := range localPromises {
		for _, entry := range promise.AcceptLog {
			seq := entry.GetSequenceNumber()

			if seq > maxSeq {
				maxSeq = seq
			}

			existing, ok := merged[seq]
			if entry.GetStatus() == "EXECUTED" {
				merged[seq] = &pb.AcceptLogEntry{
					BallotNumber: &pb.BallotNumber{
						TermNo: entry.GetBallotNumber().TermNo,
						NodeNo: entry.GetBallotNumber().NodeNo,
					},
					SequenceNumber: seq,
					AcceptValue:    entry.GetAcceptValue(),
					Status:         entry.GetStatus(),
				}
				continue
			}

			if ok {
				if existing.GetStatus() == "EXECUTED" {
					continue
				}
				if isHigherBallot(entry.GetBallotNumber(), existing.GetBallotNumber()) {
					merged[seq] = &pb.AcceptLogEntry{
						BallotNumber: &pb.BallotNumber{
							TermNo: entry.GetBallotNumber().TermNo,
							NodeNo: entry.GetBallotNumber().NodeNo,
						},
						SequenceNumber: seq,
						AcceptValue:    entry.GetAcceptValue(),
						Status:         entry.GetStatus(),
					}
				}
				continue
			}

			merged[seq] = &pb.AcceptLogEntry{
				BallotNumber: &pb.BallotNumber{
					TermNo: entry.GetBallotNumber().TermNo,
					NodeNo: entry.GetBallotNumber().NodeNo,
				},
				SequenceNumber: seq,
				AcceptValue:    entry.GetAcceptValue(),
				Status:         entry.GetStatus(),
			}
		}
	}

	// Step 2: fill gaps with no-ops
	result := make([]*pb.AcceptLogEntry, 0, maxSeq)
	for seq := int32(1); seq <= maxSeq; seq++ {
		if entry, ok := merged[seq]; ok {
			result = append(result, entry)
		} else {
			result = append(result, &pb.AcceptLogEntry{
				BallotNumber: &pb.BallotNumber{
					TermNo: n.CurrentBallot.TermNo,
					NodeNo: n.CurrentBallot.NodeNo,
				},
				SequenceNumber: seq,
				AcceptValue: &pb.ClientRequestMessage{
					MessageType:  pb.MessageType_REQUEST,
					Transaction:  &pb.Transaction{Sender: "noop", Reciever: "noop", Amount: 0},
					ClientNumber: "noop",
				},
				Status: "ACCEPTED",
			})
		}
	}

	//log.Printf("[Node %d] Merged %d promises → final log up to seq=%d",
	//	n.ID, len(localPromises), maxSeq)

	//  Print merged entries cleanly
	//log.Printf("[Node %d] ==== MERGED LOG ====", n.ID)
	//for seq, entry := range merged {
	//	txn := entry.GetAcceptValue().GetTransaction()
	//	log.Printf("Seq=%d | Seqq = %d | Ballot=(%d,%d) | Txn=%s→%s(%d) | Status=%s",
	//		seq,
	//		entry.GetSequenceNumber(),
	//		entry.BallotNumber.TermNo, entry.BallotNumber.NodeNo,
	//		txn.GetSender(), txn.GetReciever(), txn.GetAmount(),
	//		entry.GetStatus(),
	//	)
	//}

	// Print final result log cleanly
	//log.Printf("[Node %d] ==== FINAL RESULT LOG ====", n.ID)
	//for _, entry := range result {
	//	txn := entry.GetAcceptValue().GetTransaction()
	//	log.Printf("Seq=%d | Ballot=(%d,%d) | Txn=%s→%s(%d) | Status=%s",
	//		entry.SequenceNumber,
	//		entry.BallotNumber.TermNo, entry.BallotNumber.NodeNo,
	//		txn.GetSender(), txn.GetReciever(), txn.GetAmount(),
	//		entry.GetStatus(),
	//	)
	//}
	//log.Printf("[Node %d] =========================", n.ID)

	return result
}

func (n *Node) sendAcceptLogOfLeaderForActiveCatching() {
	acceptLog := n.getAcceptLogOfLeaderForCatching()
	//time.Sleep(1 * time.Second)

	var wg sync.WaitGroup
	for peerID, _ := range n.AliveClusterPeers {
		if peerID == n.ID {
			continue
		}
		addr, ok := n.Peers[peerID]
		if !ok {
			//log.Printf("[Node %d] No address found for peer %d", n.ID, peerID)
			continue
		}
		wg.Add(1)
		go func(peerID int32, peerAddr string) {
			defer wg.Done()

			conn, err := grpc.NewClient(peerAddr,
				grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				//log.Printf("[Node %d] Could not connect to peer %d for PREPARE: %v", n.ID, peerID, err)
				return
			}
			defer conn.Close()

			node := pb.NewBankApplicationClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			req := &pb.ActiveCatchUpRequest{
				AcceptLogEntries: acceptLog,
			}
			// performing active catch up based on leader log
			//log.Printf("Sending accept log of leader %d to just came to live node: %d", n.ID, peerID)
			_, err = node.PerformActiveCatchUpAsFollower(ctx, req)
			if err != nil {
				//log.Printf("[Node %d] PerformActiveCatchUpAsFollower RPC to %d failed: %v", n.ID, peerID, err)
				return
			}
		}(peerID, addr)
	}
	wg.Wait()
}

func (n *Node) getAcceptLogOfLeaderForCatching() []*pb.AcceptLogEntry {
	acceptLog := []*pb.AcceptLogEntry{}
	maxSeq := int64(0)
	for _, entry := range n.SequenceNumberToLogEntry {
		maxSeq = max(maxSeq, entry.SequenceNumber)
		if entry.Status == "ACCEPTED" || entry.Status == "COMMITTED" || entry.Status == "EXECUTED" {
			acceptLog = append(acceptLog, &pb.AcceptLogEntry{
				BallotNumber:   &pb.BallotNumber{TermNo: entry.BallotNumber.TermNo, NodeNo: entry.BallotNumber.NodeNo},
				SequenceNumber: int32(entry.SequenceNumber),
				AcceptValue:    entry.Request,
				Status:         entry.Status,
			})
		}
	}

	return acceptLog
}

func isHigherBallot(a, b *pb.BallotNumber) bool {
	if a.GetTermNo() > b.GetTermNo() {
		return true
	}
	if a.GetTermNo() == b.GetTermNo() && a.GetNodeNo() > b.GetNodeNo() {
		return true
	}
	return false
}

// broadcastNewView is invoked by the leader once it has collected quorum PROMISES.
// It sends the merged AcceptLog to all followers.
// Followers update their logs and reply with ACCEPTED for each entry.
func (n *Node) broadcastNewView(ballot *BallotNumber, mergedLog []*pb.AcceptLogEntry) {
	var wg sync.WaitGroup
	responses := make(chan *pb.NewViewResponse, len(n.AliveClusterPeers))

	for peerID, addr := range n.AliveClusterPeers {
		if peerID == n.ID {
			continue
		}
		wg.Add(1)

		go func(peerID int32, peerAddr string) {
			defer wg.Done()

			conn, err := grpc.NewClient(peerAddr,
				grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				//log.Printf("[Leader %d] Failed to connect to node %d for NEW-VIEW: %v",
				//	n.ID, peerID, err)
				return
			}
			defer conn.Close()

			follower := pb.NewBankApplicationClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			req := &pb.NewViewRequest{
				BallotNumber: &pb.BallotNumber{
					TermNo: ballot.TermNo,
					NodeNo: ballot.NodeNo,
				},
				AcceptLog: mergedLog,
			}
			n.recordMessage("NEW_VIEW", "SEND", req.GetBallotNumber(), 0, nil)

			resp, err := follower.NewView(ctx, req)
			if err != nil {
				//log.Printf("[Leader %d] NEW-VIEW RPC to node %d failed: %v", n.ID, peerID, err)
				return
			}
			for _, acc := range resp.AcceptMessageResponse {
				n.recordMessage("ACCEPTED", "RECV", acc.GetBallotNumber(), int64(acc.GetSequenceNumber()), nil)
			}

			//log.Printf("[Leader %d] Node %d acknowledged NEW-VIEW with %d accept responses",
			//	n.ID, peerID, len(resp.AcceptMessageResponse))

			responses <- resp
		}(peerID, addr)
	}

	wg.Wait()

	close(responses)

	//UPDATING your own (LEADERS) log to merged log
	for _, newEntry := range mergedLog {
		seq := newEntry.GetSequenceNumber()
		existing, ok := n.SequenceNumberToLogEntry[seq]

		if ok {
			switch existing.Status {
			case "EXECUTED":
				log.Printf("[Node %d] Seq=%d already EXECUTED skipping", n.ID, seq)

			case "COMMITTED":
				log.Printf("Ideally it should never enter here: [Node %d] Seq=%d already COMMITTED skipping", n.ID, seq)

			default:
				existing.Status = "ACCEPT"
				existing.Request = newEntry.GetAcceptValue()
				existing.BallotNumber = &BallotNumber{
					TermNo: newEntry.GetBallotNumber().GetTermNo(),
					NodeNo: newEntry.GetBallotNumber().GetNodeNo(),
				}
				existing.SequenceNumber = int64(newEntry.SequenceNumber)
				log.Printf("[Node %d] Updated Seq=%d � ACCEPTED", n.ID, seq)
			}
		} else {
			n.SequenceNumberToLogEntry[seq] = &LogEntry{
				BallotNumber: &BallotNumber{
					TermNo: newEntry.GetBallotNumber().GetTermNo(),
					NodeNo: newEntry.GetBallotNumber().GetNodeNo(),
				},
				SequenceNumber: int64(seq),
				Request:        newEntry.GetAcceptValue(),
				Status:         "ACCEPTED",
			}
			//log.Printf("[Node %d] Inserted Seq=%d as ACCEPTED", n.ID, seq)
		}
	}

	//log.Printf("LOCAL LOG STATE of leader at broadcastNewView ====")
	//for s, entry := range n.SequenceNumberToLogEntry {
	//	txn := entry.Request.GetTransaction()
	//	log.Printf("Seq=%d | Seqq=%d | Ballot=(%d,%d) | Txn=%s→%s(%d) | Status=%s",
	//		s,
	//		entry.SequenceNumber,
	//		entry.BallotNumber.TermNo, entry.BallotNumber.NodeNo,
	//		txn.GetSender(), txn.GetReciever(), txn.GetAmount(),
	//		entry.Status,
	//	)
	//}

	// Collect quorum acknowledgments for each sequence number
	ackCount := make(map[int32]int)
	for resp := range responses {
		for _, acc := range resp.AcceptMessageResponse {
			seq := acc.GetSequenceNumber()
			ackCount[seq]++
		}
	}

	// Decide commit based on majority ACKs
	for seq, count := range ackCount {
		if count+1 >= n.majority() { // +1 includes leader
			//log.Printf("[Leader %d] Seq=%d reached quorum in NEW-VIEW → committing", n.ID, seq)

			n.mu.Lock()
			if entry, ok := n.SequenceNumberToLogEntry[seq]; ok {
				//if entry.Status != "EXECUTED" {
				n.mu.Unlock()
				n.BroadcastCommit(int64(seq), ballot, entry.Request, nil)
			} else {
				n.mu.Unlock()
				//log.Printf("[Leader %d] Seq=%d missing in leader log → should not happen", n.ID, seq)
			}
		}
	}
}
