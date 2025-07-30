package main

import (
	. "github.com/tobiajo/gossip-gloomers/common"
)

type Txn struct {
	Txn []TxnOp `json:"txn"`
}

type TxnOk struct {
	Txn []TxnOp `json:"txn"`
}
