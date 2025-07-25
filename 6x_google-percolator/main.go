package main

import (
	"log"
	utils "github.com/tobiajo/gossip-gloomers/utils"
	"strconv"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	kv  *maelstrom.KV
	tso *utils.TSO
}

type transaction = []operation
type operation = [3]any

type cell struct {
	ts    int
	data  *int
	lock  *string
	write *string
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)
	tso := utils.NewLinTSO(n)
	s := server{
		kv:  kv,
		tso: tso,
	}

	utils.RegisterHandler(n, "txn", s.txnHandler)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func (s *server) txnHandler(req Txn) (TxnOk, error) {
	startTs, err := s.tso.Get()
	if err != nil {
		return *new(TxnOk), err
	}

	result := transaction{}
	primary := new(int)
	for _, op := range req.Txn {
		name := op[0].(string)
		key := int(op[1].(float64))
		if primary == nil {
			*primary = key
		}
		switch name {
		case "r":
			value, err := read(s.kv, key)
			if err != nil {
				return *new(TxnOk), err
			}
			result = append(result, operation{name, key, value})
		case "w":
			value := int(op[2].(float64))
			if err := preWrite(s.kv, key, startTs, value, primary); err != nil {
				return *new(TxnOk), err
			}
			result = append(result, operation{name, key, value})
		}
	}

	res := TxnOk{
		Txn: result,
	}
	return res, nil
}

func read(kv *maelstrom.KV, key int) (int, error) {
	return 0, nil
}

func preWrite(kv *maelstrom.KV, key int, ts int, data int, primary string) error {
	cells, err := utils.ReadOrElse(kv, strconv.Itoa(key), []cell{})
	if err != nil {
		return err
	}

	// If there’s already a lock or newer version than start_ts, the current transaction will be rolled back because of write conflict.

	cell = cell{
		ts:    ts,
		data:  &data,
		lock:  &primary,
		write: nil,
	}

	return 0, nil
}
