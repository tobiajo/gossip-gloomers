package main

import (
	"log"
	utils "maelstrom-utils"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	n                   *maelstrom.Node
	deliveredSelf       mapset.Set[message]
	unconfirmedDelivery mapset.Set[delivery]
}

type nodeID = string
type message = int

type delivery struct {
	dest    nodeID
	message message
}

// Challenge #3c: Fault Tolerant Broadcast
// https://fly.io/dist-sys/3c
func main() {
	n := maelstrom.NewNode()
	s := server{
		n:                   n,
		deliveredSelf:       mapset.NewSet[message](),
		unconfirmedDelivery: mapset.NewSet[delivery](),
	}

	go func() {
		for range time.Tick(time.Second) {
			for _, delivery := range s.unconfirmedDelivery.ToSlice() {
				err := utils.SendAsync(s.n, "deliver", delivery.dest, Deliver{delivery.message})
				if err != nil {
					return
				}
			}
		}
	}()

	// external
	utils.RegisterHandler(n, "broadcast", s.broadcastHandler)
	utils.RegisterHandler(n, "read", s.readHandler)
	utils.RegisterHandler(n, "topology", s.topologyHandler)

	// internal
	utils.RegisterHandler(n, "deliver", s.deliverHandler)
	utils.RegisterAsyncHandler(n, "deliver_ok", s.deliverOkHandler)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func (s *server) broadcastHandler(req Broadcast) (BroadcastOk, error) {
	for _, dest := range s.n.NodeIDs() {
		delivery := delivery{dest, req.Message}
		s.unconfirmedDelivery.Add(delivery)
		err := utils.SendAsync(s.n, "deliver", delivery.dest, Deliver{delivery.message})
		if err != nil {
			return *new(BroadcastOk), err
		}
	}

	res := BroadcastOk{}
	return res, nil
}

func (s *server) readHandler(req Read) (ReadOk, error) {
	res := ReadOk{
		Messages: s.deliveredSelf.ToSlice(),
	}
	return res, nil
}

func (s *server) topologyHandler(req Topology) (TopologyOk, error) {
	res := TopologyOk{}
	return res, nil
}

func (s *server) deliverHandler(req Deliver) (DeliverOk, error) {
	s.deliveredSelf.Add(req.Message)

	res := DeliverOk{
		Src:     s.n.ID(),
		Message: req.Message,
	}
	return res, nil
}

func (s *server) deliverOkHandler(req DeliverOk) error {
	delivery := delivery{req.Src, req.Message}
	s.unconfirmedDelivery.Remove(delivery)

	return nil
}
