package main

import (
	"log"
	"strconv"

	utils "github.com/tobiajo/gossip-gloomers/utils"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	self      func() nodeID
	intStream chan int
}

type nodeID = string

// Challenge #2: Unique ID Generation
// https://fly.io/dist-sys/2
func main() {
	n := maelstrom.NewNode()
	intStream := make(chan int)
	s := server{
		self:      func() nodeID { return n.ID() }, // nodeID avaliable after init
		intStream: intStream,
	}

	go func() {
		i := 0
		for {
			intStream <- i
			i += 1
		}
	}()

	utils.RegisterHandler(n, "generate", s.generateHandler)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func (s *server) generateHandler(req Generate) (GenerateOk, error) {
	res := GenerateOk{
		Id: s.self() + "-m" + strconv.Itoa(<-s.intStream),
	}
	return res, nil
}
