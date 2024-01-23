package main

import (
	"log"
	"time"

	utils "maelstrom-utils"

	mapset "github.com/deckarep/golang-set/v2"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	n                   *maelstrom.Node
	deliveredSelf       mapset.Set[message]
	unconfirmedDelivery mapset.Set[delivery]
	pendingDelivery     mapset.Set[delivery]
}

type nodeID = string
type message = int

type delivery struct {
	dest    nodeID
	message message
}

// Challenge #3d: Efficient Broadcast, Part I
// https://fly.io/dist-sys/3d
func main() {
	n := maelstrom.NewNode()
	s := server{
		n:                   n,
		deliveredSelf:       mapset.NewSet[message](),
		unconfirmedDelivery: mapset.NewSet[delivery](),
		pendingDelivery:     mapset.NewSet[delivery](),
	}

	go func() {
		for range time.Tick(time.Second) {
			for _, delivery := range s.unconfirmedDelivery.ToSlice() {
				s.pendingDelivery.Add(delivery)
			}
		}
	}()

	go func() {
		for range time.Tick(time.Second / 2) {
			messages := make(map[nodeID][]message)
			for _, delivery := range s.pendingDelivery.ToSlice() {
				if _, present := messages[delivery.dest]; !present {
					messages[delivery.dest] = []message{}
				}
				messages[delivery.dest] = append(messages[delivery.dest], delivery.message)
				s.pendingDelivery.Remove(delivery)
			}
			for dest, messages := range messages {
				err := utils.SendAsync(n, "deliver", dest, Deliver{messages})
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
		s.pendingDelivery.Add(delivery)
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
	for _, message := range req.Messages {
		s.deliveredSelf.Add(message)
	}

	res := DeliverOk{
		Src:      s.n.ID(),
		Messages: req.Messages,
	}
	return res, nil
}

func (s *server) deliverOkHandler(req DeliverOk) error {
	for _, message := range req.Messages {
		s.unconfirmedDelivery.Remove(delivery{req.Src, message})
	}

	return nil
}
