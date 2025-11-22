package node

import (
	"bank-application/internal/common"
	"context"
	"fmt"
	"log"
	"sort"
	"strconv"
	"time"

	pb "bank-application/pb/bank-application/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// NodeServer wraps a Node and implements the gRPC service
type NodeServer struct {
	pb.UnimplementedBankApplicationServer
	node *Node
}

// NewNodeServer creates a gRPC server wrapper for a node
func NewNodeServer(node *Node) *NodeServer {
	return &NodeServer{node: node}
}

func (s *NodeServer) HandleClientRequest(ctx context.Context, req *pb.ClientRequestMessage) (*pb.ClientResponseMessage, error) {
	if !s.node.isAlive {
		//log.Printf("[Node %d] Rejecting client request (node dead)", s.node.ID)
		return nil, status.Error(codes.Unavailable, "node is not alive")
	}

	s.node.mu.Lock()

	key := fmt.Sprintf("%s-%d-%s-%s-%d",
		req.GetClientNumber(),
		req.GetTime(),
		req.GetTransaction().GetSender(),
		req.GetTransaction().GetReciever(),
		req.GetTransaction().GetAmount(),
	)

	if s.node.IsLeader {
		if resp, ok := s.node.LastReplies[key]; ok {
			s.node.mu.Unlock()
			return resp, nil
		}
	}

	// Case 1: No leader known yet
	if !s.node.IsLeader && s.node.CurrentLeaderID == 0 {
		s.node.mu.Unlock()
		log.Printf("[Node %d] No leader known � rejecting until election stabilizes", s.node.ID)
		return nil, status.Error(codes.Unavailable, "leader election in progress, please retry")
	}

	// Case 2: Not leader forward to current leader
	if !s.node.IsLeader {
		log.Printf("I'm not leader so forwarding to leader node: %d", s.node.CurrentLeaderID)
		leaderAddr, ok := s.node.Peers[s.node.CurrentLeaderID]
		if !ok {
			s.node.mu.Unlock()
			log.Printf("[Node %d] No valid leader address", s.node.ID)
			return nil, status.Error(codes.FailedPrecondition, "leader unknown")
		}

		s.node.mu.Unlock()

		conn, err := grpc.NewClient(
			leaderAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			//log.Printf("[Node %d] Could not forward to leader %d: %v", s.node.ID, s.node.CurrentLeaderID, err)
			return nil, status.Error(codes.Unavailable, "leader unreachable")
		}
		defer conn.Close()

		leader := pb.NewBankApplicationClient(conn)
		forwardCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		respFromLeader, err2 := leader.HandleClientRequest(forwardCtx, req)
		if err2 != nil {
			//log.Printf("Failed when forwarded to leader in leader side: leader no: %d, error: %v", s.node.CurrentLeaderID, err2)
			return nil, err2
		}
		return respFromLeader, nil
	}

	sender := req.GetTransaction().GetSender()
	receiver := req.GetTransaction().GetReciever()

	s.node.mu.Unlock()

	if common.IsIntraShard(sender, receiver, s.node.ID) {
		return s.handleIntraShardTransaction(ctx, req, key, sender, receiver)
	}

	return s.handleCrossShardTransaction(ctx, req, key, sender, receiver)

	//if req.GetTransaction().GetAmount() == 0 {
	//	balance := s.node.Balances[sender]
	//	resp := &pb.ClientResponseMessage{
	//		MessageType:  pb.MessageType_RESPONSE,
	//		BallotNo:     &pb.BallotNumber{TermNo: s.node.CurrentBallot.TermNo, NodeNo: s.node.CurrentBallot.NodeNo},
	//		Time:         req.GetTime(),
	//		ClientNumber: req.GetClientNumber(),
	//		ResultStruct: &pb.Result{
	//			Balance:    balance,
	//			IsExecuted: true,
	//		},
	//	}
	//	// optionally cache in LastReplies for dedup
	//	s.node.LastReplies[key] = resp
	//	s.node.mu.Unlock()
	//	return resp, nil
	//}

}

func (s *NodeServer) handleIntraShardTransaction(
	ctx context.Context,
	req *pb.ClientRequestMessage,
	key string,
	sender string,
	receiver string,
) (*pb.ClientResponseMessage, error) {

	s.node.mu.Lock()
	if ok := s.node.tryLockIntraShard(sender, receiver); !ok {
		s.node.mu.Unlock()
		errMsg := fmt.Sprintf(
			"Intra transaction: (%s -> %s : %d) time: %d skipped due to lock conflict",
			req.GetTransaction().GetSender(),
			req.GetTransaction().GetReciever(),
			req.GetTransaction().GetAmount(),
			req.GetTime(),
		)

		return nil, status.Error(codes.Aborted, errMsg)
	}
	s.node.mu.Unlock()

	s.node.ImplementPaxos(req, key)

	deadline, hasDeadline := ctx.Deadline()
	for {
		if resp, ok := s.node.LastReplies[key]; ok {
			s.node.mu.Lock()
			s.node.unlockIntraShard(sender, receiver)
			s.node.mu.Unlock()

			return &pb.ClientResponseMessage{
				MessageType:  pb.MessageType_RESPONSE,
				BallotNo:     &pb.BallotNumber{TermNo: s.node.CurrentBallot.TermNo, NodeNo: s.node.CurrentBallot.NodeNo},
				Time:         req.GetTime(),
				ClientNumber: req.GetClientNumber(),
				Result:       resp.GetResult(),
			}, nil
		}

		if err := ctx.Err(); err != nil || (hasDeadline && time.Now().After(deadline)) {
			s.node.mu.Lock()
			s.node.unlockIntraShard(sender, receiver)
			s.node.mu.Unlock()
			return nil, status.Error(codes.DeadlineExceeded, "commit still pending")
		}

		s.node.replyCond.Wait()
	}
}

func (s *NodeServer) handleCrossShardTransaction(
	ctx context.Context,
	req *pb.ClientRequestMessage,
	key string,
	sender string,
	receiver string,
) (*pb.ClientResponseMessage, error) {

	// 2PC coordinator logic will go here later
	// For now we just signal it is not implemented
	return nil, status.Error(codes.Unimplemented, "cross shard transactions not implemented yet")
}

func (s *NodeServer) AcceptMessage(ctx context.Context, req *pb.AcceptMessageRequest) (*pb.AcceptMessageResponse, error) {
	if !s.node.isAlive {
		//log.Printf("[Node %d] Rejecting ACCEPT (node dead)", s.node.ID)
		return nil, status.Error(codes.Unavailable, "node is not alive")
	}

	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	//log.Printf(
	//	"[Node %d] AcceptMessage received → sequence_number: %d | Transaction: {sender: %s, receiver: %s, amount: %d} | time: %d",
	//	s.node.ID,
	//	req.SequenceNumber,
	//	req.Transaction.Sender,
	//	req.Transaction.Reciever,
	//	req.Transaction.Amount,
	//	req.ClientRequestMessage.Time,
	//)

	s.node.IsLeader = false
	s.node.CurrentLeaderID = req.GetBallotNumber().GetNodeNo()

	isValid := s.isAcceptMessageValid(req)
	s.node.recordMessageLocked("ACCEPT", "RECV", req.GetBallotNumber(), int64(req.GetSequenceNumber()), req.GetClientRequestMessage())

	if isValid {
		//log.Printf("[Node %d] Accepted seq=%d ballot=%d",
		//	s.node.ID, req.SequenceNumber, req.BallotNumber)

		s.node.resetElectionTimerOnLeaderActivity()

		entry, ok := s.node.SequenceNumberToLogEntry[req.GetSequenceNumber()]
		//crm := req.GetClientRequestMessage()
		//if crm == nil {
		//	log.Printf("[Node %d] No client request message for seq=%d for ACCEPT", s.node.ID, req.SequenceNumber)
		//}
		if !ok {
			entry2 := &LogEntry{
				BallotNumber: &BallotNumber{
					TermNo: req.GetBallotNumber().GetTermNo(),
					NodeNo: req.GetBallotNumber().GetNodeNo(),
				},
				SequenceNumber: int64(req.GetSequenceNumber()),
				Request:        req.GetClientRequestMessage(),
				Status:         "ACCEPT",
			}
			s.node.SequenceNumberToLogEntry[req.GetSequenceNumber()] = entry2
			//log.Printf("[Node %d] Created new log entry on ACCEPTED seq=%d", s.node.ID, req.GetSequenceNumber())
		} else {
			entry.Status = "ACCEPT"
			//log.Printf("[Node %d] Marked seq=%d as ACCEPTED", s.node.ID, req.GetSequenceNumber())
		}

		s.node.recordMessageLocked("ACCEPTED", "SEND", req.GetBallotNumber(), int64(req.GetSequenceNumber()), req.GetClientRequestMessage())

		return &pb.AcceptMessageResponse{
			MessageType:    pb.MessageType_ACCEPTED,
			BallotNumber:   req.BallotNumber,
			SequenceNumber: req.SequenceNumber,
			Transaction:    req.Transaction,
			NodeNumber:     s.node.ID,
		}, nil
	}

	//log.Printf("[Node %d] Rejected accept message: ballot too old", s.node.ID)
	return nil, nil
}

// CommitMessage commits the request message executed by follower
func (s *NodeServer) CommitMessage(ctx context.Context, req *pb.CommitMessageRequest) (*pb.CommitMessageResponse, error) {
	if !s.node.isAlive {
		//log.Printf("[Node %d] Rejecting COMMIT (node dead)", s.node.ID)
		return nil, status.Error(codes.Unavailable, "node is not alive")
	}

	s.node.mu.Lock()

	s.node.IsLeader = false
	s.node.CurrentLeaderID = req.GetBallotNumber().GetNodeNo()

	// Reset election timer on leader activity
	s.node.resetElectionTimerOnLeaderActivity()

	seq := req.GetSequenceNumber()
	entry, ok := s.node.SequenceNumberToLogEntry[seq]

	//crm := req.GetClientRequestMessage()
	//if crm == nil {
	//	log.Printf("[Node %d] No client request message for seq=%d for COMMIT", s.node.ID, seq)
	//}

	if !ok {
		//log.Printf("Missing accept for [Node %d], received commit message: seq=%d", s.node.ID, seq)
		entry = &LogEntry{
			BallotNumber: &BallotNumber{
				TermNo: req.GetBallotNumber().GetTermNo(),
				NodeNo: req.GetBallotNumber().GetNodeNo(),
			},
			SequenceNumber: int64(seq),
			Request:        req.GetClientRequestMessage(),
			Status:         "COMMITTED",
		}
		s.node.SequenceNumberToLogEntry[seq] = entry
		//log.Printf("[Node %d] Created new log entry on COMMITTED seq=%d", s.node.ID, seq)
	} else if entry.Status != "EXECUTED" {
		entry.Status = "COMMITTED"
		//log.Printf("[Node %d] Marked seq=%d for trnscn: (%s -> %s): %d as COMMITTED", s.node.ID, entry.Request.GetTransaction().GetSender(), entry.Request.GetTransaction().GetReciever(), entry.Request.GetTransaction().GetAmount())
	}
	s.node.recordMessageLocked("COMMIT", "RECV", req.GetBallotNumber(), int64(seq), req.GetClientRequestMessage())

	s.node.mu.Unlock()

	go s.node.executeInOrder()

	s.node.recordMessageLocked("COMMITTED", "SEND", req.GetBallotNumber(), int64(seq), req.GetClientRequestMessage())

	return &pb.CommitMessageResponse{
		MessageType:    pb.MessageType_COMMITED,
		BallotNumber:   req.BallotNumber,
		SequenceNumber: req.SequenceNumber,
	}, nil
}

// Prepare follower just stores prepare request recievied from candidate (during election) (does not promise immediately)
func (s *NodeServer) Prepare(ctx context.Context, req *pb.PrepareMessage) (*emptypb.Empty, error) {
	if !s.node.isAlive {
		//log.Printf("[Node %d] Rejecting Prepare (node dead)", s.node.ID)
		return nil, status.Error(codes.Unavailable, "node is not alive")
	}

	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	//ballot := req.GetBallotNumber()

	//log.Printf("[Node %d] Received PREPARE from node=%d ballot=(%d,%d)",
	//	s.node.ID, ballot.GetNodeNo(), ballot.GetTermNo(), ballot.GetNodeNo())

	// Just store the prepare for later evaluation
	s.node.pendingPrepares = append(s.node.pendingPrepares, req)
	s.node.recordMessageLocked("PREPARE", "RECV", req.GetBallotNumber(), 0, nil)

	s.node.lastPrepareSeen = time.Now()

	return &emptypb.Empty{}, nil
}

func (s *NodeServer) Promise(ctx context.Context, req *pb.PromiseMessage) (*emptypb.Empty, error) {
	if !s.node.isAlive {
		//log.Printf("[Node %d] Rejecting Promise (node dead)", s.node.ID)
		return nil, status.Error(codes.Unavailable, "node is not alive")
	}
	if s.node.IsLeader {
		//log.Printf("Already recivied majority of promises and I'm the leader")
		return &emptypb.Empty{}, nil
	}

	s.node.promiseMu.Lock()
	defer s.node.promiseMu.Unlock()

	s.node.recordMessage("PROMISE", "RECV", req.GetBallotNumber(), 0, nil)

	fromID := req.GetFromNodeId()

	//log.Printf("[Node %d] Received PROMISE from node=%d ballot=(%d,%d) with %d entries",
	//	s.node.ID, fromID,
	//	req.GetBallotNumber().GetTermNo(), req.GetBallotNumber().GetNodeNo(),
	//	len(req.AcceptLog))

	// Store or overwrite this node's promise
	s.node.promiseInbox[fromID] = req

	quorum := s.node.majority()
	if len(s.node.promiseInbox)+1 >= quorum && !s.node.promisesQuorumReached {
		s.node.promisesQuorumReached = true

		//log.Printf("[Node %d] Quorum of %d PROMISES reached � broadcasting NEW-VIEW",
		//	s.node.ID, quorum)

		s.node.becomeLeader(&BallotNumber{
			TermNo: req.GetBallotNumber().GetTermNo(),
			NodeNo: req.GetBallotNumber().GetNodeNo(),
		})
	}

	return &emptypb.Empty{}, nil
}

func (s *NodeServer) NewView(ctx context.Context, req *pb.NewViewRequest) (*pb.NewViewResponse, error) {
	if !s.node.isAlive {
		//log.Printf("[Node %d] Rejecting NewView (node dead)", s.node.ID)
		return nil, status.Error(codes.Unavailable, "node is not alive")
	}

	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	s.node.resetElectionTimerOnLeaderActivity()

	s.node.NewViewMessages = append(s.node.NewViewMessages, req)
	s.node.recordMessageLocked("NEW_VIEW", "RECV", req.GetBallotNumber(), 0, nil)

	s.node.IsLeader = false
	s.node.CurrentLeaderID = req.GetBallotNumber().GetNodeNo()

	//log.Printf("[Node %d] Received NEW-VIEW from leader=(%d,%d) with %d log entries",
	//	s.node.ID,
	//	req.GetBallotNumber().GetTermNo(),
	//	req.GetBallotNumber().GetNodeNo(),
	//	len(req.AcceptLog))

	s.node.CurrentBallot.TermNo = req.GetBallotNumber().GetTermNo()
	s.node.CurrentBallot.NodeNo = req.GetBallotNumber().GetNodeNo()

	var responses []*pb.AcceptMessageResponse

	for _, newEntry := range req.AcceptLog {
		seq := newEntry.GetSequenceNumber()
		existing, ok := s.node.SequenceNumberToLogEntry[seq]

		var ballotNumber *pb.BallotNumber

		if ok {
			switch existing.Status {
			case "EXECUTED":
				if existing.BallotNumber != nil {
					ballotNumber = &pb.BallotNumber{
						TermNo: existing.BallotNumber.TermNo,
						NodeNo: existing.BallotNumber.NodeNo,
					}
				}
				//log.Printf("[Node %d] Seq=%d already EXECUTED skipping", s.node.ID, seq)

			case "COMMITTED":
				if existing.BallotNumber != nil {
					ballotNumber = &pb.BallotNumber{
						TermNo: existing.BallotNumber.TermNo,
						NodeNo: existing.BallotNumber.NodeNo,
					}
				}
				//log.Printf("Ideally it should never enter here: [Node %d] Seq=%d already COMMITTED skipping", s.node.ID, seq)

			default:
				existing.Status = "ACCEPT"
				existing.Request = newEntry.GetAcceptValue()
				existing.BallotNumber = &BallotNumber{
					TermNo: newEntry.GetBallotNumber().GetTermNo(),
					NodeNo: newEntry.GetBallotNumber().GetNodeNo(),
				}
				existing.SequenceNumber = int64(newEntry.SequenceNumber)
				ballotNumber = &pb.BallotNumber{
					TermNo: newEntry.GetBallotNumber().GetTermNo(),
					NodeNo: newEntry.GetBallotNumber().GetNodeNo(),
				}
				//log.Printf("[Node %d] Updated Seq=%d ACCEPTED", s.node.ID, seq)
			}
		} else {
			s.node.SequenceNumberToLogEntry[seq] = &LogEntry{
				BallotNumber: &BallotNumber{
					TermNo: newEntry.GetBallotNumber().GetTermNo(),
					NodeNo: newEntry.GetBallotNumber().GetNodeNo(),
				},
				SequenceNumber: int64(seq),
				Request:        newEntry.GetAcceptValue(),
				Status:         "ACCEPTED",
			}
			ballotNumber = &pb.BallotNumber{
				TermNo: newEntry.GetBallotNumber().GetTermNo(),
				NodeNo: newEntry.GetBallotNumber().GetNodeNo(),
			}
			//log.Printf("[Node %d] Inserted Seq=%d as ACCEPTED", s.node.ID, seq)
		}

		responses = append(responses, &pb.AcceptMessageResponse{
			MessageType:    pb.MessageType_ACCEPTED,
			BallotNumber:   ballotNumber,
			SequenceNumber: seq,
			Transaction:    newEntry.GetAcceptValue().GetTransaction(),
			NodeNumber:     s.node.ID,
		})
		s.node.recordMessageLocked("ACCEPTED", "SEND", newEntry.GetBallotNumber(), int64(seq), newEntry.GetAcceptValue())
	}

	//log.Printf("[Node %d] LOCAL LOG STATE AFTER NEW-VIEW :::", s.node.ID)
	//for seq, entry := range s.node.SequenceNumberToLogEntry {
	//	txn := entry.Request.GetTransaction()
	//	log.Printf("Seq=%d | Seqq=%d | Ballot=(%d,%d) | Txn=%s→%s(%d) | Status=%s",
	//		seq,
	//		entry.SequenceNumber,
	//		entry.BallotNumber.TermNo, entry.BallotNumber.NodeNo,
	//		txn.GetSender(), txn.GetReciever(), txn.GetAmount(),
	//		entry.Status,
	//	)
	//}
	//log.Printf("[Node %d] =========================================", s.node.ID)

	return &pb.NewViewResponse{
		AcceptMessageResponse: responses,
	}, nil
}

func (s *NodeServer) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	if !s.node.isAlive {
		//log.Printf("[Node %d] Rejecting Heartbeat (node dead)", s.node.ID)
		return nil, status.Error(codes.Unavailable, "node is not alive")
	}

	s.node.mu.Lock()

	log.Printf("[Node %d] Received heartbeat from leader=%d → reset election timer",
		s.node.ID, req.GetLeaderId())

	//s.node.recordMessageLocked("HEARTBEAT", "RECV", &pb.BallotNumber{
	//	TermNo: s.node.CurrentBallot.TermNo,
	//	NodeNo: req.GetLeaderId(),
	//}, 0, nil)

	s.node.IsLeader = false
	s.node.CurrentLeaderID = req.GetLeaderId()

	s.node.mu.Unlock()

	s.node.resetElectionTimerOnLeaderActivity()

	return &pb.HeartbeatResponse{Ack: true}, nil
}

func (s *NodeServer) UpdateNodeStatus(ctx context.Context, req *pb.AliveRequest) (*pb.AliveResponse, error) {
	n := s.node

	wasAlive := n.isAlive
	wasLeader := n.IsLeader

	n.isAlive = req.GetAlive()
	becameAlive := !wasAlive && n.isAlive
	wentDown := wasAlive && !n.isAlive

	address := n.Peers[n.ID]
	if n.isAlive {
		found := false
		for _, id := range n.AliveNodes {
			if id == s.node.ID {
				found = true
				break
			}
		}
		if !found {
			n.AliveNodes = append(n.AliveNodes, s.node.ID)
		}
		n.AliveClusterPeers[n.ID] = address
	} else {
		newList := make([]int32, 0, len(n.AliveNodes))
		for _, id := range n.AliveNodes {
			if id != s.node.ID {
				newList = append(newList, id)
			}
		}
		n.AliveNodes = newList
		delete(n.AliveClusterPeers, n.ID)
	}

	if (wentDown && wasLeader) || (wasLeader && (len(n.AliveClusterPeers) < n.majority())) {
		n.IsLeader = false
		n.CurrentLeaderID = 0
		//log.Printf("[Node %d] Node went down while leader stopping heartbeats", s.node.ID)
		s.node.stopHeartbeats()
	} else if wentDown {
		n.isAlive = false
		if n.electionTimer != nil {
			n.electionTimer.Stop()
			n.electionTimer = nil
		}
	}
	//if wasLeader && !wentDown {
	//	// current leader
	//	// to catch the previous log entries
	//	//log.Printf("Calling: sendAcceptLogOfLeaderForActiveCatching")
	//	//if len(n.AliveNodes) >= n.majority() {
	//	//	s.node.sendAcceptLogOfLeaderForActiveCatching()
	//	//}
	//	s.node.ScheduleNextElection()
	//}

	if becameAlive {
		// for active catching of node
		if len(s.node.AliveClusterPeers) >= n.majority() {
			log.Printf("Became alive and now I update my log to leader's log")
			s.UpdateMyLogToLeadersLog()
		}
		s.node.ScheduleNextElection()
	}

	//if n.isAlive {
	//	s.node.ScheduleNextElection()
	//}

	return &pb.AliveResponse{Success: true}, nil
}

func (s *NodeServer) FailCurrentLeader(ctx context.Context, req *pb.FailCurrentLeaderRequest) (*pb.FailCurrentLeaderResponse, error) {
	if !s.node.isAlive {
		return nil, status.Error(codes.Unavailable, "node is not alive")
	}

	s.node.mu.Lock()
	if !s.node.IsLeader {
		s.node.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, "not the current leader")
	}

	s.node.IsLeader = false
	s.node.CurrentLeaderID = 0
	s.node.isAlive = false
	s.node.mu.Unlock()

	//log.Printf("Calling from: FailCurrentLeader")
	s.node.stopHeartbeats()

	return &pb.FailCurrentLeaderResponse{}, nil
}

func (s *NodeServer) PerformActiveCatchUpAsFollower(ctx context.Context, req *pb.ActiveCatchUpRequest) (*pb.ActiveCatchUpResponse, error) {
	if !s.node.isAlive {
		return nil, status.Error(codes.Unavailable, "node is not alive: Unable to perform active catchup")
	}
	//log.Printf("Inside : PerformActiveCatchUpAsFollower")

	//s.node.mu.Lock()
	//defer s.node.mu.Unlock()

	for _, leaderEntry := range req.GetAcceptLogEntries() {
		seq := leaderEntry.GetSequenceNumber()
		leaderStatus := leaderEntry.GetStatus()

		if leaderStatus != "COMMITTED" && leaderStatus != "EXECUTED" {
			//log.Printf("Ideally it shld not happen: [Node %d] Skipping seq=%d because leader status=%s (not COMMITTED/EXECUTED)",
			//	s.node.ID, seq, leaderStatus)
			continue
		}

		followerEntry, exists := s.node.SequenceNumberToLogEntry[seq]

		if !exists {
			newEntry := &LogEntry{
				BallotNumber: &BallotNumber{
					TermNo: leaderEntry.GetBallotNumber().GetTermNo(),
					NodeNo: leaderEntry.GetBallotNumber().GetNodeNo(),
				},
				SequenceNumber: int64(seq),
				Request:        leaderEntry.GetAcceptValue(),
				Status:         "COMMITTED",
			}

			s.node.SequenceNumberToLogEntry[seq] = newEntry
			//log.Printf("[Node %d] Inserted missing log entry seq=%d from leader → COMMITTED", s.node.ID, seq)

		} else if followerEntry.Status != "EXECUTED" {
			fTxn := followerEntry.Request.GetTransaction()
			lTxn := leaderEntry.GetAcceptValue().GetTransaction()

			if (fTxn.GetSender() != lTxn.GetSender() ||
				fTxn.GetReciever() != lTxn.GetReciever() ||
				fTxn.GetAmount() != lTxn.GetAmount()) &&
				(followerEntry.Status == "ACCEPTED" ||
					followerEntry.Status == "COMMITTED" ||
					followerEntry.Status == "EXECUTED") {

				//log.Printf("!!This should not happen as it violates consensus!!")
			}

			followerEntry.Request = leaderEntry.GetAcceptValue()
			followerEntry.Status = "COMMITTED"
			//log.Printf("[Node %d] Updated seq=%d → COMMITTED", s.node.ID, seq)
		}
		//else {
		//	log.Printf("[Node %d] Skipping seq=%d (already EXECUTED)", s.node.ID, seq)
		//}
	}

	s.node.executeInOrder()

	return nil, nil
}

func (s *NodeServer) GetLeaderLogForActiveCatching(ctx context.Context, req *pb.GetLeaderLogRequest) (*pb.GetLeaderLogResponse, error) {
	n := s.node

	n.mu.Lock()

	if !n.isAlive {
		n.mu.Unlock()
		return nil, status.Error(codes.Unavailable, "node is not alive")
	}

	if !n.IsLeader {
		n.mu.Unlock()
		return &pb.GetLeaderLogResponse{
			IsLeader:         false,
			AcceptLogEntries: nil,
		}, nil
	}

	n.mu.Unlock()
	entries := s.node.getAcceptLogOfLeaderForCatching()

	return &pb.GetLeaderLogResponse{
		IsLeader:         true,
		AcceptLogEntries: entries,
	}, nil
}

func (s *NodeServer) ReadClientBalance(ctx context.Context, req *pb.ReadClientBalanceRequest) (*pb.ReadClientBalanceResponse, error) {
	balance := s.node.Balances[req.GetClientId()]
	resp := &pb.ReadClientBalanceResponse{
		Balance: balance,
	}
	return resp, nil
}

// ------------------------------------CLIENT FUNCTIONS--------------------------------------------------------
func (s *NodeServer) PrintBalance(ctx context.Context, req *pb.PrintBalanceRequest) (*pb.PrintBalanceResponse, error) {
	balance := s.node.Balances[req.GetClientId()]
	resp := &pb.PrintBalanceResponse{
		Balance: balance,
	}

	node := s.node

	var seqs []int32
	for seq := range node.SequenceNumberToLogEntry {
		seqs = append(seqs, seq)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })

	fmt.Println("----------PRINTING LOG--------------")
	for _, seq := range seqs {
		entry := node.SequenceNumberToLogEntry[seq]
		if entry == nil || entry.Request == nil {
			continue
		}

		txn := entry.Request.Transaction

		fmt.Printf(
			"Seq: %d | Status: %s | Txn: %s -> %s (%d) | Time: %d\n",
			seq,
			entry.Status,
			txn.Sender,
			txn.Reciever,
			txn.Amount,
			entry.Request.Time,
		)
	}

	return resp, nil
}

func (s *NodeServer) GetStatus(ctx context.Context, req *pb.StatusRequest) (*pb.StatusResponse, error) {
	//if !s.node.isAlive {
	//	//log.Printf("[Node %d] Rejecting GetStatus (node dead)", s.node.ID)
	//	return nil, status.Error(codes.Unavailable, "node is not alive")
	//}

	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	log.Printf("[Node %d] ==== Current Log State ====", s.node.ID)
	seqNums := make([]int, 0, len(s.node.SequenceNumberToLogEntry))
	for _, seq := range s.node.SequenceNumberToLogEntry {
		seqNums = append(seqNums, int(seq.SequenceNumber))
	}

	sort.Ints(seqNums)

	for _, seq := range seqNums {
		entry := s.node.SequenceNumberToLogEntry[int32(seq)]
		if entry.Request != nil && entry.Request.Transaction != nil {
			txn := entry.Request.Transaction
			log.Printf("Seq=%d (ActualSeq=%d) -> %s -> %s (%d) -> %s",
				seq,
				entry.SequenceNumber,
				txn.Sender,
				txn.Reciever,
				txn.Amount,
				entry.Status,
			)
		} else {
			log.Printf("Seq=%d -> <no txn> -> %s", seq, entry.Status)
		}
	}
	log.Printf("[Node %d] ==========================", s.node.ID)

	seq := req.GetSequenceNumber()
	statusStr := "X"

	if entry, ok := s.node.SequenceNumberToLogEntry[seq]; ok {
		switch entry.Status {
		case "ACCEPTED":
			statusStr = "A"
		case "COMMITTED":
			statusStr = "C"
		case "EXECUTED":
			statusStr = "E"
		}
	}

	statusResponse := &pb.StatusResponse{
		NodeId:         s.node.ID,
		SequenceNumber: seq,
		Status:         statusStr,
	}
	if entry2, ok2 := s.node.SequenceNumberToLogEntry[seq]; ok2 {
		if entry2.Request != nil {
			statusResponse.ClientRequestMessage = entry2.Request
		}
	}
	return statusResponse, nil
}

func (s *NodeServer) GetDB(ctx context.Context, req *pb.DBRequest) (*pb.DBResponse, error) {
	//if !s.node.isAlive {
	//	//log.Printf("[Node %d] Rejecting GetDB (node dead)", s.node.ID)
	//	return nil, status.Error(codes.Unavailable, "node is not alive")
	//}

	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	return &pb.DBResponse{
		NodeId:   s.node.ID,
		Balances: s.node.Balances,
	}, nil
}

func (s *NodeServer) PrintLog(ctx context.Context, req *pb.PrintLogRequest) (*pb.PrintLogResponse, error) {
	//if !s.node.isAlive {
	//	//log.Printf("[Node %d] Rejecting GetLog (node dead)", s.node.ID)
	//	return nil, status.Error(codes.Unavailable, "node is not alive")
	//}

	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	resp := &pb.PrintLogResponse{
		LogEntryMessage: []*pb.LogEntryMessage{},
	}

	for _, entry := range s.node.AllMessages {
		logEntry := &pb.LogEntryMessage{
			SequenceNumber: int32(entry.SequenceNumber),
			BallotNumber: &pb.BallotNumber{
				TermNo: entry.BallotNumber.TermNo,
				NodeNo: entry.BallotNumber.NodeNo,
			},
			ClientRequest: entry.Request,
			Status:        entry.Status,
		}
		resp.LogEntryMessage = append(resp.LogEntryMessage, logEntry)
	}

	return resp, nil
}

func (s *NodeServer) PrintView(ctx context.Context, req *pb.PrintViewRequest) (*pb.PrintViewResponse, error) {
	//if !s.node.isAlive {
	//	//log.Printf("[Node %d] Rejecting PrintView (node dead)", s.node.ID)
	//	return nil, status.Error(codes.Unavailable, "node is not alive")
	//}

	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	if len(s.node.NewViewMessages) == 0 {
		//log.Printf("[Node %d] No NEW-VIEW messages recorded yet.", s.node.ID)
		return &pb.PrintViewResponse{NewViewRequest: []*pb.NewViewRequest{}}, nil
	}

	log.Printf("[Node %d] Returning %d NEW-VIEW messages", s.node.ID, len(s.node.NewViewMessages))

	resp := &pb.PrintViewResponse{
		NewViewRequest: make([]*pb.NewViewRequest, len(s.node.NewViewMessages)),
	}
	copy(resp.NewViewRequest, s.node.NewViewMessages)

	return resp, nil
}

func (s *NodeServer) FlushPreviousDataAndUpdatePeersStatus(ctx context.Context, req *pb.FlushAndUpdateStatusRequest) (*emptypb.Empty, error) {
	n := s.node

	aliveNodesReq := req.GetLiveNodes()
	aliveSet := make(map[int32]struct{}, len(aliveNodesReq))
	for _, id := range aliveNodesReq {
		aliveSet[id] = struct{}{}
	}

	n.mu.Lock()
	n.Peers = common.Peers

	balances := make(map[string]int32, 3000)
	start, end := common.ShardRangeForNode(n.ID)
	for acc := start; acc <= end; acc++ {
		key := strconv.Itoa(int(acc))
		balances[key] = 10
	}
	n.Balances = balances

	n.CurrentBallot = &BallotNumber{
		TermNo: 0,
		NodeNo: n.ID,
	}
	n.SequenceNumber = 0
	n.SequenceNumberToLogEntry = make(map[int32]*LogEntry)

	n.LastExecutedSequenceNumber = 0
	n.LastReplies = make(map[string]*pb.ClientResponseMessage)

	n.promiseInbox = make(map[int32]*pb.PromiseMessage)
	n.promisesQuorumReached = false
	n.pendingPrepares = nil

	n.NewViewMessages = []*pb.NewViewRequest{}
	n.AllMessages = []*LogEntry{}

	n.lastPrepareSeen = time.Time{}
	n.lastElectionStarted = time.Time{}

	n.LockTable = make(map[string]bool)

	n.AliveNodes = aliveNodesReq

	if _, ok := aliveSet[n.ID]; ok {
		n.isAlive = true
	} else {
		n.isAlive = false
	}

	myCluster := common.ClusterOf(n.ID)
	n.AliveClusterPeers = make(map[int32]string)
	for peerID, addr := range n.Peers {
		if common.ClusterOf(peerID) == myCluster {
			if _, ok := aliveSet[peerID]; ok {
				n.AliveClusterPeers[peerID] = addr
			}
		}
	}

	n.IsLeader = false
	n.CurrentLeaderID = 0

	if n.electionTimer != nil {
		n.electionTimer.Stop()
		n.electionTimer = nil
	}
	if n.heartbeatTicker != nil {
		n.heartbeatTicker.Stop()
		n.heartbeatTicker = nil
	}

	n.mu.Unlock()

	if n.isAlive {
		n.ScheduleNextElection()
	}

	return &emptypb.Empty{}, nil
}
