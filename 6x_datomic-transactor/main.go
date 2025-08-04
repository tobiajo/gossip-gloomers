package main

import (
	"context"
	"log"

	. "github.com/tobiajo/gossip-gloomers/common"
	utils "github.com/tobiajo/gossip-gloomers/utils"

	uuid "github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	kv *maelstrom.KV
}

type transaction = []TxnOp

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)
	s := server{kv: kv}

	utils.RegisterHandler(n, "txn", s.txnHandler)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func (s *server) txnHandler(req Txn) (TxnOk, error) {
	txn, err := transact(s.kv, req.Txn)
	if err != nil {
		return *new(TxnOk), err
	}

	res := TxnOk{
		Txn: txn,
	}
	return res, nil
}

func transact(kv *maelstrom.KV, txn []TxnOp) ([]TxnOp, error) {
	stateRef, err := utils.ReadOrElse(kv, "STATE_REF", "")
	if err != nil {
		return nil, err
	}
	state, err := utils.ReadOrElse(kv, stateRef, map[int]int{})
	if err != nil {
		return nil, err
	}

	result := transaction{}
	for _, op := range txn {
		switch op.Op {
		case "r":
			value := state[op.Key]
			result = append(result, NewTxnOp(op.Op, op.Key, &value))
		case "w":
			state[op.Key] = *op.Value
			result = append(result, NewTxnOp(op.Op, op.Key, op.Value))
		}
	}

	updatedStateRef := uuid.New().String()
	err = kv.Write(context.Background(), updatedStateRef, state)
	if err != nil {
		return nil, err
	}
	err = kv.CompareAndSwap(context.Background(), "STATE_REF", stateRef, updatedStateRef, true)
	if err != nil {
		return transact(kv, txn) // retry on CAS failure
	}

	return result, nil
}
