package main

import (
	"context"
	"log"
	utils "maelstrom-utils"

	uuid "github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	kv *maelstrom.KV
}

type transaction = []operation
type operation = [3]any

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
	for err != nil {
		txn, err = transact(s.kv, req.Txn)
	}

	res := TxnOk{
		Txn: txn,
	}
	return res, nil
}

func transact(kv *maelstrom.KV, txn transaction) (transaction, error) {
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
		name := op[0].(string)
		key := int(op[1].(float64))
		switch name {
		case "r":
			value := state[key]
			result = append(result, operation{name, key, value})
		case "w":
			value := int(op[2].(float64))
			state[key] = value
			result = append(result, operation{name, key, value})
		}
	}

	updatedStateRef := uuid.New().String()
	err = kv.Write(context.Background(), updatedStateRef, state)
	if err != nil {
		return nil, err
	}
	err = kv.CompareAndSwap(context.Background(), "STATE_REF", stateRef, updatedStateRef, true)
	if err != nil {
		return nil, err
	}

	return result, nil
}
