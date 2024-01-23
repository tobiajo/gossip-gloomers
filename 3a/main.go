package main

import (
	"log"
	utils "maelstrom-utils"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	mu            sync.Mutex
	deliveredSelf []message
}

type message = int

// Challenge #3a: Single-Node Broadcast
// https://fly.io/dist-sys/3a
func main() {
	n := maelstrom.NewNode()
	s := server{}

	utils.RegisterHandler(n, "broadcast", s.broadcastHandler)
	utils.RegisterHandler(n, "read", s.readHandler)
	utils.RegisterHandler(n, "topology", s.topologyHandler)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func (s *server) broadcastHandler(req Broadcast) (BroadcastOk, error) {
	s.mu.Lock()
	s.deliveredSelf = append(s.deliveredSelf, req.Message)
	s.mu.Unlock()

	res := BroadcastOk{}
	return res, nil
}

func (s *server) readHandler(req Read) (ReadOk, error) {
	res := ReadOk{
		Messages: s.deliveredSelf,
	}
	return res, nil
}

func (s *server) topologyHandler(req Topology) (TopologyOk, error) {
	res := TopologyOk{}
	return res, nil
}
