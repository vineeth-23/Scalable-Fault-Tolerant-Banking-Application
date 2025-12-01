package main

import (
	"bank-application/internal/database"
	"bank-application/internal/node"
	"flag"
	"fmt"
	"log"
)

func main() {
	peers := map[int32]string{
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

	id := flag.Int("id", 1, "node ID (1-9)")
	flag.Parse()

	database.InitRedisClient("localhost:6379")
	fmt.Println("Connected to Redis at localhost:6379")

	address, ok := peers[int32(*id)]
	if !ok {
		log.Fatalf("Invalid node ID: %d", *id)
	}

	n := node.NewNode(int32(*id), address, peers)
	srv := node.NewNodeServer(n)

	srv.StartServer()
}
