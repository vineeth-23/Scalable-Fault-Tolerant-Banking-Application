package node

import (
	"context"
	"fmt"
	"sync"
	"time"

	pb "bank-application/pb/bank-application/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func (n *Node) ImplementPaxos(req *pb.ClientRequestMessage, clientTransactionKey string) {
	n.mu.Lock()
	duplicate := false
	for _, entry := range n.SequenceNumberToLogEntry {
		if entry.Request != nil {
			t1 := entry.Request.GetTransaction()
			t2 := req.GetTransaction()
			if t1.GetSender() == t2.GetSender() &&
				t1.GetReciever() == t2.GetReciever() &&
				t1.GetAmount() == t2.GetAmount() &&
				entry.Request.GetTime() == req.GetTime() &&
				(entry.Status == "ACCEPTED" || entry.Status == "COMMITTED" || entry.Status == "EXECUTED") {
				duplicate = true
				break
			}
		}
	}
	if duplicate {
		n.mu.Unlock()
		return
	}

	var maxSeq int64
	for _, ent := range n.SequenceNumberToLogEntry {
		if ent.SequenceNumber > maxSeq {
			maxSeq = ent.SequenceNumber
		}
	}
	seq := maxSeq + 1

	ballotNumber := &BallotNumber{
		TermNo: n.CurrentBallot.TermNo,
		NodeNo: n.CurrentBallot.NodeNo,
	}
	n.SequenceNumber = seq
	AcceptLogEntry := &LogEntry{
		BallotNumber:   ballotNumber,
		SequenceNumber: int64(seq),
		Status:         "ACCEPT",
		Request:        req,
	}
	n.SequenceNumberToLogEntry[int32(seq)] = AcceptLogEntry

	//log.Printf("client request transaction recievied is from %s to %s of %d and sequence is %d", req.GetTransaction().GetSender(), req.GetTransaction().GetReciever(), req.GetTransaction().GetAmount(), n.SequenceNumber)
	n.mu.Unlock()

	// Build accept message
	accept := &pb.AcceptMessageRequest{
		MessageType:          pb.MessageType_ACCEPT,
		SequenceNumber:       int32(seq),
		Transaction:          req.Transaction,
		ClientRequestMessage: req,
	}

	accept.BallotNumber = &pb.BallotNumber{
		TermNo: ballotNumber.TermNo,
		NodeNo: ballotNumber.NodeNo,
	}

	// Send accept to all peers
	var wg sync.WaitGroup
	responses := make(chan *pb.AcceptMessageResponse, len(n.AliveClusterPeers))

	n.recordMessage("ACCEPT", "SEND", accept.GetBallotNumber(), int64(accept.GetSequenceNumber()), req)

	for _, addr := range n.AliveClusterPeers {
		if addr == n.Address {
			continue
		}
		wg.Add(1)
		go func(peerAddr string) {
			defer wg.Done()
			conn, err := grpc.NewClient(
				peerAddr,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			if err != nil {
				//log.Printf("Failed to connect to %s: %v", peerAddr, err)
				return
			}
			defer conn.Close()

			follower := pb.NewBankApplicationClient(conn)

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			resp, err := follower.AcceptMessage(ctx, accept)

			if err != nil {
				//log.Printf("RPC failed to %s: %v", peerAddr, err)
				return
			}

			responses <- resp
		}(addr)
	}

	wg.Wait()
	close(responses)

	ackCount := 1
	for resp := range responses {
		if resp != nil {
			ackCount++
			n.recordMessage("ACCEPTED", "RECV", resp.GetBallotNumber(), int64(resp.GetSequenceNumber()), nil)
		}
	}

	quorum := n.majority()

	duplicate = false
	var originalLogEntry *LogEntry
	n.mu.Lock()
	for _, entry := range n.SequenceNumberToLogEntry {
		if entry.Request != nil {
			t1 := entry.Request.GetTransaction()
			t2 := req.GetTransaction()
			if t1.GetSender() == t2.GetSender() &&
				t1.GetReciever() == t2.GetReciever() &&
				t1.GetAmount() == t2.GetAmount() &&
				entry.Request.GetTime() == req.GetTime() &&
				(entry.Status == "ACCEPTED" || entry.Status == "COMMITTED" || entry.Status == "EXECUTED") {
				duplicate = true
				originalLogEntry = entry
				break
			}
		}
	}
	n.mu.Unlock()

	//if duplicate {
	//	log.Printf("duplicate accept request for %s", req.GetTransaction().GetSender())
	//}

	if ackCount >= quorum && !duplicate {
		//log.Printf("[Leader %d] Majority accepted seq=%d, committing...", n.ID, seq)
		n.mu.Lock()

		if entry, ok := n.SequenceNumberToLogEntry[int32(seq)]; ok {
			entry.Status = "ACCEPTED"
		} else {
			//log.Printf("[Node %d] Missing log entry for seq=%d, creating one", n.ID, seq)

			n.SequenceNumberToLogEntry[int32(seq)] = &LogEntry{
				BallotNumber:   ballotNumber,
				SequenceNumber: int64(seq),
				Request:        req,
				Status:         "ACCEPTED",
			}
		}
		n.mu.Unlock()

		//log.Printf("Calling BroadcastCommit")
		n.BroadcastCommit(int64(seq), ballotNumber, req)
		//log.Printf("BroadcastCommit complete")
	} else if duplicate && originalLogEntry.Status == "ACCEPTED" {
		//log.Printf("Already got accepted, now calling BroadcastCommit")
		n.BroadcastCommit(originalLogEntry.SequenceNumber, originalLogEntry.BallotNumber, originalLogEntry.Request)
	} else if duplicate && originalLogEntry.Status == "COMMITTED" {
		//log.Printf("Already got COMMITTED, now calling execute")
		n.executeInOrder()
	}

	//log.Printf("[Leader %s] Not enough acks for seq=%d", n.ID, seq)
}

func (n *Node) BroadcastCommit(seq int64, ballotNumber *BallotNumber, req *pb.ClientRequestMessage) {
	var wg sync.WaitGroup
	responses := make(chan *pb.CommitMessageResponse, len(n.AliveClusterPeers))

	for nodeID, addr := range n.AliveClusterPeers {
		if nodeID == n.ID {
			continue
		}
		wg.Add(1)
		go func(peerID int32, peerAddr string) {
			defer wg.Done()

			conn, err := grpc.NewClient(
				peerAddr,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			if err != nil {
				//log.Printf("Failed to connect to node %d (%s): %v", peerID, peerAddr, err)
				return
			}
			defer conn.Close()

			follower := pb.NewBankApplicationClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			commitReq := &pb.CommitMessageRequest{
				MessageType:          pb.MessageType_COMMIT,
				SequenceNumber:       int32(seq),
				Transaction:          req.GetTransaction(),
				ClientRequestMessage: req,
				BallotNumber: &pb.BallotNumber{
					TermNo: ballotNumber.TermNo,
					NodeNo: ballotNumber.NodeNo,
				},
			}
			n.recordMessage("COMMIT", "SEND", commitReq.GetBallotNumber(), int64(commitReq.GetSequenceNumber()), commitReq.GetClientRequestMessage())

			resp, err := follower.CommitMessage(ctx, commitReq)
			if err != nil {
				//log.Printf("Commit RPC failed to node %d: %v", peerID, err)
				return
			}
			responses <- resp
		}(nodeID, addr)
	}

	wg.Wait()
	close(responses)

	for resp := range responses {
		if resp != nil {
			n.recordMessage("COMMITTED", "RECV", resp.GetBallotNumber(), int64(resp.GetSequenceNumber()), req)
		}
	}

	// Mark entry as COMMITTED (don’t wait for majority to execute)
	n.mu.Lock()
	duplicate := false
	for _, entry := range n.SequenceNumberToLogEntry {
		if entry.Request != nil {
			t1 := entry.Request.GetTransaction()
			t2 := req.GetTransaction()
			if t1.GetSender() == t2.GetSender() &&
				t1.GetReciever() == t2.GetReciever() &&
				t1.GetAmount() == t2.GetAmount() &&
				entry.Request.GetTime() == req.GetTime() &&
				(entry.Status == "COMMITTED" || entry.Status == "EXECUTED") {
				duplicate = true
				break
			}
		}
	}

	//if duplicate {
	//	log.Printf("[Node %d] Duplicate committed message for trnscn: %s->%s (%d) at time: %s",req.GetTransaction().GetSender() req.GetTime())
	//}

	if entry, ok := n.SequenceNumberToLogEntry[int32(seq)]; ok {
		if !duplicate && entry.Status != "EXECUTED" {
			entry.Status = "COMMITTED"
		}
	} else if !duplicate {
		//log.Printf("[Leader %d] Missing log entry for seq=%d, creating one", n.ID, seq)
		n.SequenceNumberToLogEntry[int32(seq)] = &LogEntry{
			BallotNumber:   ballotNumber,
			SequenceNumber: seq,
			Request:        req,
			Status:         "COMMITTED",
		}
	}
	n.mu.Unlock()

	//log.Printf("[Leader %d] ==== LOCAL LOG STATE at BroadcastCommit ====", n.ID)
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

	//log.Printf("Calling executeInOrder")
	// Try to execute in order (including catch-up)
	n.executeInOrder()
}

func (n *Node) executeInOrder() {
	n.mu.Lock()
	defer n.mu.Unlock()

	for {
		//log.Printf("Last executed sequence number for node %d is %d", n.ID, n.LastExecutedSequenceNumber)
		nextSeq := n.LastExecutedSequenceNumber + 1
		entry, ok := n.SequenceNumberToLogEntry[nextSeq]
		if !ok || (entry.Status != "COMMITTED" && entry.Status != "EXECUTED") {
			s := fmt.Sprintf("[Node %d] Cannot execute seq=%d yet → waiting for commit", n.ID, nextSeq)
			if entry != nil &&
				entry.Request != nil &&
				entry.Request.GetTransaction() != nil {
				txn := entry.Request.GetTransaction()
				if txn.GetSender() != "" && txn.GetReciever() != "" {
					s += fmt.Sprintf(" of trnscn: %s → %s, amt=%d",
						txn.GetSender(),
						txn.GetReciever(),
						txn.GetAmount(),
					)
				}
			}
			//log.Println(s)
			break // stop if next not ready
		}

		txn := entry.Request.GetTransaction()
		if txn.GetSender() == "noop" {
			entry.Status = "EXECUTED"
			n.LastExecutedSequenceNumber = nextSeq
			continue
		}
		key := fmt.Sprintf("%s-%d-%s-%s-%d",
			entry.Request.GetClientNumber(),
			entry.Request.GetTime(),
			txn.Sender, txn.Reciever, txn.Amount,
		)

		if _, isPresent := n.LastReplies[key]; isPresent {
			n.LastExecutedSequenceNumber = nextSeq
			//log.Printf("[Leader %d] Duplicate client request detected, returning cached result", n.ID)
			continue
		}

		//added here
		if entry.Status == "EXECUTED" {
			n.LastExecutedSequenceNumber = nextSeq
			//log.Printf("Ideally shld not come here: Already executed seq=%d with key = %s", nextSeq, key)
			continue
		}

		// Execute
		isSuccess := false
		if isTransactionValid(txn) {
			isSuccess = n.applyTransaction(txn, entry)
		}
		entry.Status = "EXECUTED"
		n.LastExecutedSequenceNumber = nextSeq

		resp := &pb.ClientResponseMessage{
			MessageType: pb.MessageType_RESPONSE,
			BallotNo: &pb.BallotNumber{
				TermNo: entry.BallotNumber.TermNo,
				NodeNo: entry.BallotNumber.NodeNo,
			},
			Time:         entry.Request.GetTime(),
			ClientNumber: entry.Request.GetClientNumber(),
			Result:       isSuccess,
		}

		n.LastReplies[key] = resp

		//CSV UPDATE
		//n.updateBalancesCSV()

		n.replyCond.Broadcast()

		//log.Printf("[Leader %d] ==== LOCAL LOG STATE At executeInOrder func ====", n.ID)
		//for s, entry1 := range n.SequenceNumberToLogEntry {
		//	txn1 := entry1.Request.GetTransaction()
		//	log.Printf("Seq=%d | Seqq=%d | Ballot=(%d,%d) | Txn=%s→%s(%d) | Status=%s",
		//		s,
		//		entry1.SequenceNumber,
		//		entry1.BallotNumber.TermNo, entry1.BallotNumber.NodeNo,
		//		txn1.GetSender(), txn1.GetReciever(), txn1.GetAmount(),
		//		entry1.Status,
		//	)
		//}

		//log.Printf("[Node %d] Executed seq=%d txn=%s -> %s (%d)",
		//	n.ID, nextSeq, txn.GetSender(), txn.GetReciever(), txn.GetAmount())
	}
}

func (n *Node) applyTransaction(txn *pb.Transaction, logEntry *LogEntry) bool {
	sender := txn.Sender
	receiver := txn.Reciever
	amt := txn.Amount

	if n.Balances[sender] >= amt {
		n.Balances[sender] -= amt
		n.Balances[receiver] += amt
		//log.Printf("[Node %d] Applied txn: %s -> %s : %d", n.ID, sender, receiver, amt)
		return true
	}
	//log.Printf("[Node %d] Failed txn (insufficient funds): %s -> %s : %d",
	//	n.ID, sender, receiver, amt)
	return false
}
