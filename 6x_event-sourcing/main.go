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
	for err != nil {
		txnLog, err = appendTxnLog(s.kv, req.Txn)
	}
	state := recoverState(txnLog)

	result := transaction{}
	for _, op := range req.Txn {
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

	res := TxnOk{
		Txn: result,
	}
	return res, nil
}

func appendTxnLog(kv *maelstrom.KV, txn []operation) ([]transaction, error) {
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
		return *new([]transaction), err
	}

	return txnLog, nil
}

func recoverState(txnLog []transaction) map[int]int {
	state := map[int]int{}
	for _, txn := range txnLog {
		for _, op := range txn {
			name := op[0].(string)
			key := int(op[1].(float64))
			switch name {
			case "w":
				value := int(op[2].(float64))
				state[key] = value
			}
		}
	}
	return state
}
