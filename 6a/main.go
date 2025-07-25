package main

import (
	"log"
	utils "github.com/tobiajo/gossip-gloomers/utils"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	state map[int]int
	mu    sync.Mutex
}

type transaction = []operation
type operation = [3]any

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
		name := op[0].(string)
		key := int(op[1].(float64))
		switch name {
		case "r":
			value := s.state[key]
			result = append(result, operation{name, key, value})
		case "w":
			value := int(op[2].(float64))
			s.state[key] = value
			result = append(result, operation{name, key, value})
		}
	}

	res := TxnOk{
		Txn: result,
	}
	return res, nil
}
