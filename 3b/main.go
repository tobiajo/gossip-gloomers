package main

import (
	"log"
	utils "github.com/tobiajo/gossip-gloomers/utils"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	n             *maelstrom.Node
	mu            sync.Mutex
	deliveredSelf []message
}

type message = int

// Challenge #3b: Multi-Node Broadcast
// https://fly.io/dist-sys/3b
func main() {
	n := maelstrom.NewNode()
	s := server{
		n: n,
	}

	// external
	utils.RegisterHandler(n, "broadcast", s.broadcastHandler)
	utils.RegisterHandler(n, "read", s.readHandler)
	utils.RegisterHandler(n, "topology", s.topologyHandler)

	// internal
	utils.RegisterAsyncHandler(n, "deliver", s.deliverHandler)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func (s *server) broadcastHandler(req Broadcast) (*BroadcastOk, error) {
	deliverReq := Deliver{req.Message}
	for _, dest := range s.n.NodeIDs() {
		err := utils.SendAsync(s.n, "deliver", dest, deliverReq)
		if err != nil {
			return nil, err
		}
	}

	s.mu.Lock()
	s.deliveredSelf = append(s.deliveredSelf, req.Message)
	s.mu.Unlock()

	res := BroadcastOk{}
	return &res, nil
}

func (s *server) readHandler(req Read) (*ReadOk, error) {
	res := ReadOk{
		Messages: s.deliveredSelf,
	}
	return &res, nil
}

func (s *server) topologyHandler(req Topology) (*TopologyOk, error) {
	res := TopologyOk{}
	return &res, nil
}

func (s *server) deliverHandler(req Deliver) error {
	s.mu.Lock()
	s.deliveredSelf = append(s.deliveredSelf, message(req.Message))
	s.mu.Unlock()

	return nil
}
