package main

import (
	"context"
	"log"
	utils "maelstrom-utils"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	self    func() nodeID
	cluster func() []nodeID
	kv      *maelstrom.KV
	mu      sync.Mutex
}

type nodeID = string

// Challenge #4: Grow-Only Counter
// https://fly.io/dist-sys/4
func main() {
	n := maelstrom.NewNode()
	s := server{
		self:    func() nodeID { return n.ID() },
		cluster: func() []nodeID { return n.NodeIDs() },
		kv:      maelstrom.NewSeqKV(n),
	}

	utils.RegisterHandler(n, "add", s.addHandler)
	utils.RegisterHandler(n, "read", s.readHandler)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func (s *server) addHandler(req Add) (AddOk, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	nodeValue, err := utils.ReadOrElse(s.kv, s.self(), 0)
	if err != nil {
		return *new(AddOk), err
	}
	err = s.kv.Write(context.Background(), s.self(), nodeValue+req.Delta)
	if err != nil {
		return *new(AddOk), err
	}

	res := AddOk{}
	return res, nil
}

func (s *server) readHandler(req Read) (ReadOk, error) {
	value := 0
	for _, node := range s.cluster() {
		nodeValue, err := utils.ReadOrElse(s.kv, node, 0)
		if err != nil {
			return *new(ReadOk), err
		}
		value += nodeValue
	}

	res := ReadOk{
		Value: value,
	}
	return res, nil
}
