package main

import (
	"log"
	"sync"

	. "github.com/tobiajo/gossip-gloomers/common"
	utils "github.com/tobiajo/gossip-gloomers/utils"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	state map[int]int
	mu    sync.Mutex
}

type transaction = []TxnOp

// Challenge #6a: Single-Node, Totally-Available Transactions
// https://fly.io/dist-sys/6a
func main() {
	n := maelstrom.NewNode()
	s := server{state: map[int]int{}}

	utils.RegisterHandler(n, "txn", s.txnHandler)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func (s *server) txnHandler(req Txn) (TxnOk, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := transaction{}
	for _, op := range req.Txn {
		switch op.Op {
		case "r":
			value := s.state[op.Key]
			result = append(result, NewTxnOp(op.Op, op.Key, &value))
		case "w":
			s.state[op.Key] = *op.Value
			result = append(result, NewTxnOp(op.Op, op.Key, op.Value))
		}
	}

	res := TxnOk{
		Txn: result,
	}
	return res, nil
}
