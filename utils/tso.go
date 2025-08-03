package utils

import (
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type TSO struct {
	node *maelstrom.Node
}

// NewLinTSO returns a client to the linearizable timestamp oracle.
func NewLinTSO(node *maelstrom.Node) *TSO {
	return &TSO{
		node: node,
	}
}

func (tso *TSO) Get() (int, error) {
	res, err := Send[req, res](tso.node, "ts", "lin-tso", req{})
	if err != nil {
		return *new(int), err
	}
	return res.Ts, nil
}

type req struct{}

type res struct {
	Ts int `json:"ts"`
}
