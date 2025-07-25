package main

import (
	"log"
	utils "github.com/tobiajo/gossip-gloomers/utils"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Challenge #1: Echo
// https://fly.io/dist-sys/1
func main() {
	n := maelstrom.NewNode()

	utils.RegisterHandler(n, "echo", echoHandler)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func echoHandler(req Echo) (EchoOk, error) {
	res := EchoOk{
		Echo: req.Echo,
	}
	return res, nil
}
