package main

import (
	"encoding/json"

	. "github.com/tobiajo/gossip-gloomers/common"
)

type Txn struct {
	Txn []TxnOp `json:"txn"`
}

type TxnOk struct {
	Txn []TxnOp `json:"txn"`
}

func (txn Txn) String() string {
	b, err := json.Marshal(txn)
	if err != nil {
		return "<Txn: invalid JSON>"
	}
	return string(b)
}
