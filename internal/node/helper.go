package node

import (
	"bank-application/internal/common"
	"bank-application/pb/bank-application/pb"
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func (s *NodeServer) isAcceptMessageValid(req *pb.AcceptMessageRequest) bool {
	if s.node.CurrentBallot.TermNo > req.GetBallotNumber().GetTermNo() {
		return false
	}
	if s.node.CurrentBallot.TermNo == req.GetBallotNumber().GetTermNo() && s.node.CurrentBallot.NodeNo > req.GetBallotNumber().GetNodeNo() {
		return false
	}
	return true
}

func isTransactionValid(transaction *pb.Transaction) bool {
	if transaction.GetSender() == "noop" {
		return false
	}
	return true
}

// becomeLeader marks this node as leader and broadcasts NEW-VIEW
func (n *Node) becomeLeader(ballot *BallotNumber) {
	n.mu.Lock()

	//log.Printf("node no: %d becomeLeader with term number: %d", ballot.NodeNo, ballot.TermNo)
	// Check if some higher ballot already won
	if isHigherBallot(&pb.BallotNumber{
		TermNo: n.CurrentBallot.TermNo,
		NodeNo: n.CurrentBallot.NodeNo,
	}, &pb.BallotNumber{
		TermNo: ballot.TermNo,
		NodeNo: ballot.NodeNo,
	}) {
		//log.Printf("Ideally should not happen:: [Node %d] Cannot become leader with ballot=(%d,%d), already seen higher ballot=(%d,%d)",
		//	n.ID, ballot.TermNo, ballot.NodeNo,
		//	n.CurrentBallot.TermNo, n.CurrentBallot.NodeNo)
		n.mu.Unlock()
		return
	}

	// Mark self as leader
	n.IsLeader = true
	n.CurrentLeaderID = ballot.NodeNo
	n.CurrentBallot = ballot

	n.mu.Unlock()

	n.startHeartbeats()

	//log.Printf("[Node %d] Became leader with ballot=(%d,%d)", n.ID, ballot.TermNo, ballot.NodeNo)

	go func() {
		mergedLog := n.mergePromiseLogs()
		//log.Printf("[Node %d] Merged log has %d entries:", n.ID, len(mergedLog))
		//for i, entry := range mergedLog {
		//	log.Printf(
		//		"  [%d] Seq=%d | Ballot=(%d,%d) | Txn=(%s -> %s, %d) | Status=%s",
		//		i,
		//		entry.SequenceNumber,
		//		entry.BallotNumber.TermNo,
		//		entry.BallotNumber.NodeNo,
		//		entry.AcceptValue.Transaction.Sender,
		//		entry.AcceptValue.Transaction.Reciever,
		//		entry.AcceptValue.Transaction.Amount,
		//		entry.Status,
		//	)
		//}

		n.broadcastNewView(&BallotNumber{
			TermNo: ballot.TermNo,
			NodeNo: ballot.NodeNo,
		}, mergedLog)

		//n.mu.Lock()
		//n.promisesQuorumReached = false
		//n.promiseInbox = make(map[int32]*pb.PromiseMessage)
		//n.mu.Unlock()
	}()
}

// intiated by the leader
func (n *Node) startHeartbeats() {
	n.mu.Lock()
	// If already running, stop first
	if n.heartbeatTicker != nil {
		//log.Printf("Ideally it should not come here as there will be no time that we trigger this func when already another node is running hearbeat ,Node %d has already started heartbeat ticker", n.ID)
		n.mu.Unlock()
		return
	}

	interval := n.electionTimeoutMin / 6
	// updated here
	//if interval < 50*time.Millisecond {
	//	interval = 50 * time.Millisecond // lower bound safeguard
	//}

	n.heartbeatTicker = time.NewTicker(interval)
	n.heartbeatStopCh = make(chan struct{})
	//log.Printf("[Leader %d] Starting heartbeats every %v", n.ID, interval)
	n.mu.Unlock()

	go func() {
		for {
			select {
			case <-n.heartbeatTicker.C:
				n.mu.Lock()
				if !n.IsLeader || !n.isAlive {
					n.mu.Unlock()
					continue // only skip sending this beat
				}
				ballot := &pb.BallotNumber{
					TermNo: n.CurrentBallot.TermNo,
					NodeNo: n.CurrentBallot.NodeNo,
				}
				n.mu.Unlock()

				// Broadcast heartbeat to followers
				for peerID, addr := range n.AliveClusterPeers {
					if peerID == n.ID {
						continue
					}
					go func(peerID int32, peerAddr string) {
						conn, err := grpc.NewClient(peerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
						if err != nil {
							//log.Printf("[Leader %d] Heartbeat failed to node %d: %v", n.ID, peerID, err)
							return
						}
						defer conn.Close()

						follower := pb.NewBankApplicationClient(conn)
						ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
						defer cancel()

						req := &pb.HeartbeatRequest{
							BallotNumber: ballot,
							LeaderId:     n.ID,
						}

						_, err = follower.Heartbeat(ctx, req)
						//if err != nil {
						//	log.Printf("[Leader %d] Heartbeat RPC to node %d failed: %v", n.ID, peerID, err)
						//}
					}(peerID, addr)
				}

			case <-n.heartbeatStopCh:
				n.mu.Lock()
				if n.heartbeatTicker != nil {
					n.heartbeatTicker.Stop()
					n.heartbeatTicker = nil
				}
				n.mu.Unlock()
				//log.Printf("[Leader %d]  Heartbeats stopped (stop channel closed)", n.ID)
				return
			}
		}
	}()
}

// stopHeartbeats stops the heartbeat ticker and signals the heartbeat goroutine to exit.
// It's safe to call multiple times and avoids closing the stop channel under the lock.
func (n *Node) stopHeartbeats() {
	//log.Printf("Stopping heartbeat for node: %d", n.ID)
	n.mu.Lock()
	// if no ticker, nothing to do
	if n.heartbeatTicker == nil {
		n.mu.Unlock()
		return
	}

	// capture & clear the resources under the lock
	stopCh := n.heartbeatStopCh
	n.heartbeatStopCh = nil

	// stop and clear ticker while still under lock to avoid races on the ticker field
	n.heartbeatTicker.Stop()
	n.heartbeatTicker = nil
	n.mu.Unlock()

	// close channel outside the lock to avoid deadlocks and double-close races
	if stopCh != nil {
		close(stopCh)
	}

	//log.Printf("[Node %d] Heartbeats stopped", n.ID)
}

func (n *Node) recordMessage(kind, direction string, ballot *pb.BallotNumber, sequence int64, req *pb.ClientRequestMessage) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.appendMessageUnlocked(kind, direction, ballot, sequence, req)
}

func (n *Node) recordMessageLocked(kind, direction string, ballot *pb.BallotNumber, sequence int64, req *pb.ClientRequestMessage) {
	n.appendMessageUnlocked(kind, direction, ballot, sequence, req)
}

func (n *Node) appendMessageUnlocked(kind, direction string, ballot *pb.BallotNumber, sequence int64, req *pb.ClientRequestMessage) {
	status := strings.ToUpper(direction) + "_" + strings.ToUpper(kind)

	var internal *BallotNumber
	switch {
	case ballot != nil:
		internal = &BallotNumber{
			TermNo: ballot.GetTermNo(),
			NodeNo: ballot.GetNodeNo(),
		}
	case n.CurrentBallot != nil:
		internal = &BallotNumber{
			TermNo: n.CurrentBallot.TermNo,
			NodeNo: n.CurrentBallot.NodeNo,
		}
	default:
		internal = &BallotNumber{}
	}

	entry := &LogEntry{
		BallotNumber:   internal,
		SequenceNumber: sequence,
		Request:        req,
		Status:         status,
	}
	n.AllMessages = append(n.AllMessages, entry)
}

func (n *Node) updateBalancesCSV() {
	fileName := "C:/Users/skotha/Downloads/Proj-1_Balances.csv"
	clients := []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"}

	nodeRows := make(map[int][]string)

	// Read existing CSV
	if _, err := os.Stat(fileName); err == nil {
		file, err := os.Open(fileName)
		if err == nil {
			reader := csv.NewReader(file)
			records, _ := reader.ReadAll()
			file.Close()

			if len(records) > 1 {
				for _, r := range records[1:] { // skip header
					if len(r) < len(clients)+1 {
						continue
					}
					nodeID, err := strconv.Atoi(r[0])
					if err == nil {
						nodeRows[nodeID] = r
					}
				}
			}
		}
	}

	// Update only this node’s row
	newRow := []string{strconv.Itoa(int(n.ID))}
	for _, c := range clients {
		newRow = append(newRow, fmt.Sprintf("%d", n.Balances[c]))
	}
	nodeRows[int(n.ID)] = newRow

	// Write updated CSV
	file, err := os.Create(fileName)
	if err != nil {
		//log.Printf("Failed to create %s: %v", fileName, err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	header := append([]string{"NodeID"}, clients...)
	_ = writer.Write(header)

	for i := 1; i <= 5; i++ {
		if row, ok := nodeRows[i]; ok {
			_ = writer.Write(row)
		} else {
			emptyRow := []string{strconv.Itoa(i)}
			for range clients {
				emptyRow = append(emptyRow, "0")
			}
			_ = writer.Write(emptyRow)
		}
	}

	//log.Printf("[Node %d] Balances updated in CSV successfully", n.ID)
}

func (s *NodeServer) UpdateMyLogToLeadersLog() {
	n := s.node

	n.mu.Lock()
	peerAddrs := make(map[int32]string, len(n.AliveClusterPeers))
	for id, addr := range n.AliveClusterPeers {
		peerAddrs[id] = addr
	}
	n.mu.Unlock()

	for peerID, addr := range peerAddrs {
		if peerID == n.ID {
			continue
		}

		conn, err := grpc.NewClient(
			addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			continue
		}

		node := pb.NewBankApplicationClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := node.GetLeaderLogForActiveCatching(ctx, &pb.GetLeaderLogRequest{})
		cancel()
		conn.Close()

		if err != nil {
			continue
		}
		if !resp.GetIsLeader() {
			continue
		}

		entries := resp.GetAcceptLogEntries()
		if len(entries) == 0 {
			log.Printf("Leader log is empty -> So no more active catching")
			return
		}

		n.mu.Lock()
		for _, e := range entries {
			if e == nil {
				continue
			}
			seq := int32(e.GetSequenceNumber())

			if existing, ok := n.SequenceNumberToLogEntry[seq]; ok {
				if existing.Status == "EXECUTED" || existing.Status == "COMMITTED" {
					continue
				}
			}

			n.SequenceNumberToLogEntry[seq] = &LogEntry{
				BallotNumber: &BallotNumber{
					TermNo: e.GetBallotNumber().GetTermNo(),
					NodeNo: e.GetBallotNumber().GetNodeNo(),
				},
				SequenceNumber: int64(e.GetSequenceNumber()),
				Request:        e.GetAcceptValue(),
				Status:         "COMMITTED",
			}
		}
		n.mu.Unlock()

		n.executeInOrder()

		return
	}
}

func (n *Node) clientIDForThisNode(req *pb.ClientRequestMessage) string {
	tx := req.GetTransaction()
	sender := tx.GetSender()
	receiver := tx.GetReciever()

	start, end := common.ShardRangeForNode(n.ID)

	if sender != "" && sender != common.SenderNotValid {
		if sid, err := strconv.Atoi(sender); err == nil {
			if int32(sid) >= start && int32(sid) <= end {
				return sender
			}
		}
	}

	if receiver != "" && receiver != common.ReceiverNotValid {
		if rid, err := strconv.Atoi(receiver); err == nil {
			if int32(rid) >= start && int32(rid) <= end {
				return receiver
			}
		}
	}

	return ""
}
