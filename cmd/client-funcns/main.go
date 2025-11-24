package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sort"
	"strconv"
	"time"

	pb "bank-application/pb/bank-application/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var peers = map[int32]string{
	1: "localhost:50051",
	2: "localhost:50052",
	3: "localhost:50053",
	4: "localhost:50054",
	5: "localhost:50055",
	6: "localhost:50056",
	7: "localhost:50057",
	8: "localhost:50058",
	9: "localhost:50059",
}

const clientControlAddr = "localhost:6000"

func PrintDB(nodeID int32) {
	if nodeID == 0 {
		nodeIDs := make([]int32, 0, len(peers))
		for id := range peers {
			nodeIDs = append(nodeIDs, id)
		}
		sort.Slice(nodeIDs, func(i, j int) bool { return nodeIDs[i] < nodeIDs[j] })

		for _, id := range nodeIDs {
			printSingleDB(id, peers[id])
		}
	} else {
		addr, ok := peers[nodeID]
		if !ok {
			return
		}
		printSingleDB(nodeID, addr)
	}
}

func printSingleDB(nodeID int32, addr string) {
	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return
	}
	defer conn.Close()

	node := pb.NewBankApplicationClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := node.GetDB(ctx, &pb.DBRequest{})
	if err != nil {
		return
	}

	fmt.Printf("\n[Node %d DB]\n", resp.NodeId)

	accounts := make([]string, 0, len(resp.Balances))
	for acc := range resp.Balances {
		accounts = append(accounts, acc)
	}
	sort.Slice(accounts, func(i, j int) bool {
		ai, _ := strconv.Atoi(accounts[i])
		aj, _ := strconv.Atoi(accounts[j])
		return ai < aj
	})

	for _, acc := range accounts {
		fmt.Printf("  %s: %d\n", acc, resp.Balances[acc])
	}
}

func PrintLog(nodeID int32) {
	if nodeID == 0 {
		for id, addr := range peers {
			printSingleLog(id, addr)
		}
	} else {
		addr, ok := peers[nodeID]
		if !ok {
			return
		}
		printSingleLog(nodeID, addr)
	}
}

func printSingleLog(nodeID int32, addr string) {
	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return
	}
	defer conn.Close()

	node := pb.NewBankApplicationClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := node.PrintLog(ctx, &pb.PrintLogRequest{})
	if err != nil {
		return
	}
	if resp.GetLogEntryMessage() == nil || len(resp.GetLogEntryMessage()) == 0 {
		return
	}

	fmt.Printf("\n[Node %d Log]\n", nodeID)
	for _, e := range resp.GetLogEntryMessage() {
		t := e.ClientRequest.GetTransaction()
		b := e.GetBallotNumber()
		fmt.Printf("  Seq=%d, Ballot=(%d,%d), Status=%s, Txn=%s->%s(%d)\n",
			e.GetSequenceNumber(),
			b.GetTermNo(), b.GetNodeNo(),
			e.GetStatus(),
			t.GetSender(), t.GetReciever(), t.GetAmount(),
		)
	}
}

func PrintStatus(seq, nodeID int32) {
	if nodeID == 0 {
		for id, addr := range peers {
			printSingleStatus(seq, id, addr)
		}
	} else {
		addr, ok := peers[nodeID]
		if !ok {
			return
		}
		printSingleStatus(seq, nodeID, addr)
	}
}

func printSingleStatus(seq, nodeID int32, addr string) {
	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return
	}
	defer conn.Close()

	client := pb.NewBankApplicationClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := client.GetStatus(ctx, &pb.StatusRequest{SequenceNumber: seq})
	if err != nil {
		return
	}

	fmt.Printf("\n[Node %d Status]\n", resp.NodeId)
	fmt.Printf("  Seq=%d → %s\n", resp.SequenceNumber, resp.Status)
	if resp.ClientRequestMessage != nil {
		t := resp.ClientRequestMessage.Transaction
		fmt.Printf("  Txn: %s -> %s (%d)\n", t.Sender, t.Reciever, t.Amount)
	}
}

func PrintView(nodeID int32) {
	if nodeID == 0 {
		for id, addr := range peers {
			printSingleView(id, addr)
		}
	} else {
		addr, ok := peers[nodeID]
		if !ok {
			return
		}
		printSingleView(nodeID, addr)
	}
}

func printSingleView(nodeID int32, addr string) {
	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return
	}
	defer conn.Close()

	node := pb.NewBankApplicationClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := node.PrintView(ctx, &pb.PrintViewRequest{})
	if err != nil {
		log.Printf("PrintView failed for node %d: %v", nodeID, err)
		return
	}

	fmt.Printf("\n========= NEW-VIEW HISTORY for Node %d =========\n", nodeID)
	for i, msg := range resp.NewViewRequest {
		fmt.Printf("\n[%d] Ballot=(%d,%d), Entries=%d\n",
			i+1,
			msg.GetBallotNumber().GetTermNo(),
			msg.GetBallotNumber().GetNodeNo(),
			len(msg.GetAcceptLog()),
		)
		for _, entry := range msg.GetAcceptLog() {
			t := entry.GetAcceptValue().GetTransaction()
			fmt.Printf("   Seq=%d, Txn=%s->%s(%d), Ballot=(%d,%d)\n",
				entry.GetSequenceNumber(),
				t.GetSender(),
				t.GetReciever(),
				t.GetAmount(),
				entry.GetBallotNumber().GetTermNo(),
				entry.GetBallotNumber().GetNodeNo(),
			)
		}
	}
}

func printSingleBalance(clientID, nodeID int32, addr string) {
	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		fmt.Printf("  Node %d → connection failed: %v\n", nodeID, err)
		return
	}
	defer conn.Close()

	node := pb.NewBankApplicationClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req := &pb.PrintBalanceRequest{
		ClientId: fmt.Sprintf("%d", clientID),
	}

	resp, err := node.PrintBalance(ctx, req)
	if err != nil {
		fmt.Printf("  Node %d → read error: %v\n", nodeID, err)
		return
	}

	fmt.Printf("  Node %d → Balance: %d \n",
		nodeID, resp.Balance)
}

func PrintBalances(clientID int32) {
	if clientID <= 0 {
		fmt.Println("Please provide a valid --client <id>")
		return
	}

	clusterNodes := GetClusterForClient(clientID)
	if len(clusterNodes) == 0 {
		fmt.Printf("Client %d does not belong to any cluster\n", clientID)
		return
	}

	fmt.Printf("\n[Balances for Client %d]\n", clientID)

	sort.Slice(clusterNodes, func(i, j int) bool {
		return clusterNodes[i] < clusterNodes[j]
	})

	for _, nodeID := range clusterNodes {
		addr, ok := peers[nodeID]
		if !ok {
			continue
		}
		printSingleBalance(clientID, nodeID, addr)
	}
}

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

func PrintReshard() {
	conn, err := grpc.NewClient(
		clientControlAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Printf("failed to connect to client-control server at %s: %v", clientControlAddr, err)
		return
	}
	defer conn.Close()

	ctrl := pb.NewClientControlClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := ctrl.GetReshardPlan(ctx, &pb.GetReshardPlanRequest{})
	if err != nil {
		log.Printf("GetReshardPlan RPC failed: %v", err)
		return
	}

	if len(resp.Moves) == 0 {
		fmt.Println("[Reshard] No moves suggested")
		return
	}

	fmt.Println("[Reshard] Suggested data movements:")
	for _, m := range resp.Moves {
		fmt.Printf("(%d,c%d,c%d)\n", m.Account, m.FromCluster, m.ToCluster)
	}
}

func main() {
	mode := flag.String("mode", "db", "mode: db | log | status | view")
	nodeID := flag.Int("node", 0, "node ID (0 for all)")
	seq := flag.Int("seq", 0, "sequence number (for status)")
	clientID := flag.Int("client", 0, "client ID for balances")
	flag.Parse()

	switch *mode {
	case "db":
		PrintDB(int32(*nodeID))

	case "log":
		PrintLog(int32(*nodeID))

	case "status":
		PrintStatus(int32(*seq), int32(*nodeID))

	case "view":
		PrintView(int32(*nodeID))

	case "balances":
		PrintBalances(int32(*clientID))

	case "reshard":
		PrintReshard()

	default:
		log.Fatalf("Unknown mode: %s", *mode)
	}
}
