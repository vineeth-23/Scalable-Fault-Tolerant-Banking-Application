package client

import (
	"encoding/csv"
	"os"
	"strconv"
	"strings"
)

type CommandType string

const (
	CommandTypeFail     CommandType = "FAIL"
	CommandTypeRecover  CommandType = "RECOVER"
	CommandTypeRead     CommandType = "READ"
	CommandTypeTransfer CommandType = "TRANSFER"
)

type Command struct {
	Type   CommandType
	NodeID int32
}

type Txn struct {
	Sender   string
	Reciever string
	Amount   int32
	Time     int32
	Command  Command
}

type TxnSet struct {
	Transactions []*Txn
	LiveNodes    []string
}

func ParseCSV(path string) map[int]*TxnSet {
	file, _ := os.Open(path)
	defer file.Close()

	reader := csv.NewReader(file)
	rows, _ := reader.ReadAll()

	result := make(map[int]*TxnSet)

	var currentSet int
	var currentLiveNodes []string
	var globalTime int32 = 0

	for _, row := range rows[1:] {

		if strings.TrimSpace(row[0]) != "" {
			f, _ := strconv.ParseFloat(row[0], 64)
			currentSet = int(f)

			result[currentSet] = &TxnSet{
				Transactions: []*Txn{},
				LiveNodes:    []string{},
			}
		}

		if strings.TrimSpace(row[2]) != "" {
			live := strings.Trim(row[2], "[] ")
			nodes := strings.Split(live, ",")
			for i := range nodes {
				nodes[i] = strings.TrimSpace(nodes[i])
			}
			currentLiveNodes = nodes
			result[currentSet].LiveNodes = currentLiveNodes
		}

		raw := strings.TrimSpace(row[1])
		if raw == "" {
			continue
		}

		cell := strings.TrimSpace(raw)
		if strings.HasPrefix(cell, "'") {
			cell = strings.TrimPrefix(cell, "'")
			cell = strings.TrimSpace(cell)
		}

		var txn *Txn

		if strings.HasPrefix(cell, "F(") && strings.HasSuffix(cell, ")") {
			nodeStr := strings.TrimSuffix(strings.TrimPrefix(cell, "F("), ")")
			nodeID := parseNodeID(nodeStr)
			globalTime++

			txn = &Txn{
				Time: globalTime,
				Command: Command{
					Type:   CommandTypeFail,
					NodeID: nodeID,
				},
			}

		} else if strings.HasPrefix(cell, "R(") && strings.HasSuffix(cell, ")") {
			nodeStr := strings.TrimSuffix(strings.TrimPrefix(cell, "R("), ")")
			nodeID := parseNodeID(nodeStr)
			globalTime++

			txn = &Txn{
				Time: globalTime,
				Command: Command{
					Type:   CommandTypeRecover,
					NodeID: nodeID,
				},
			}

		} else if strings.HasPrefix(cell, "(") && strings.HasSuffix(cell, ")") {
			content := strings.Trim(cell, "()")
			parts := strings.Split(content, ",")
			
			if len(parts) == 1 {
				clientID := strings.TrimSpace(parts[0])
				globalTime++

				txn = &Txn{
					Time:   globalTime,
					Sender: clientID,
					Command: Command{
						Type: CommandTypeRead,
					},
				}

			} else if len(parts) == 3 {
				sender := strings.TrimSpace(parts[0])
				receiver := strings.TrimSpace(parts[1])
				amt := parseAmount(parts[2])
				globalTime++

				txn = &Txn{
					Time:     globalTime,
					Sender:   sender,
					Reciever: receiver,
					Amount:   amt,
					Command: Command{
						Type: CommandTypeTransfer,
					},
				}
			}
		}

		if txn != nil {
			result[currentSet].Transactions = append(result[currentSet].Transactions, txn)
		}
	}

	return result
}

func parseAmount(s string) int32 {
	v, _ := strconv.Atoi(strings.TrimSpace(s))
	return int32(v)
}

func parseNodeID(s string) int32 {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "n")
	v, _ := strconv.Atoi(s)
	return int32(v)
}
