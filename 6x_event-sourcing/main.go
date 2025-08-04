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
	s := server{
		kv: kv,
	}

	utils.RegisterHandler(n, "txn", s.txnHandler)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func (s *server) txnHandler(req Txn) (TxnOk, error) {
	txnLog, err := appendTxnLog(s.kv, req.Txn)
	if err != nil {
		return *new(TxnOk), err
	}
	state := recoverState(txnLog)

	result := transaction{}
	for _, op := range req.Txn {
		switch op.Op {
		case "r":
			value := state[op.Key]
			result = append(result, NewTxnOp(op.Op, op.Key, &value))
		case "w":
			state[op.Key] = *op.Value
			result = append(result, NewTxnOp(op.Op, op.Key, op.Value))
		}
	}

	res := TxnOk{
		Txn: result,
	}
	return res, nil
}

func appendTxnLog(kv *maelstrom.KV, txn transaction) ([]transaction, error) {
	txnLogRef, err := utils.ReadOrElse(kv, "TXN_LOG_REF", "")
	if err != nil {
		return *new([]transaction), err
	}
	txnLog, err := utils.ReadOrElse(kv, txnLogRef, []transaction{})
	if err != nil {
		return *new([]transaction), err
	}

	updatedTxnLogRef := uuid.New().String()
	updatedTxnLog := append(txnLog, txn)
	err = kv.Write(context.Background(), updatedTxnLogRef, updatedTxnLog)
	if err != nil {
		return *new([]transaction), err
	}
	err = kv.CompareAndSwap(context.Background(), "TXN_LOG_REF", txnLogRef, updatedTxnLogRef, true)
	if err != nil {
		return appendTxnLog(kv, txn) // retry on CAS failure
	}

	return txnLog, nil
}

func recoverState(txnLog []transaction) map[int]int {
	state := map[int]int{}
	for _, txn := range txnLog {
		for _, op := range txn {
			switch op.Op {
			case "w":
				state[op.Key] = *op.Value
			}
		}
	}
	return state
}
