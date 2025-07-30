package main

import (
	"context"
	"errors"
	"log"
	"strconv"

	. "github.com/tobiajo/gossip-gloomers/common"
	utils "github.com/tobiajo/gossip-gloomers/utils"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	kv  *maelstrom.KV
	tso *utils.TSO
}

type transaction = []TxnOp

type cell struct {
	Ts    int    `json:"ts"`
	Data  *int   `json:"data"`
	Lock  *int   `json:"lock"` // key of the transaction that locked this cell
	Write *write `json:"write"`
}

type write struct {
	DataTs int       `json:"data_ts"`
	Kind   writeKind `json:"kind"`
}

type writeKind string

const (
	writePut      writeKind = "Put"
	writeDelete   writeKind = "Delete"
	writeRollback writeKind = "Rollback"
)

// https://tikv.org/deep-dive/distributed-transaction/percolator/

// TODO test with happy path and then commit
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
	// Prewrite phase
	startTs, err := s.tso.Get()
	if err != nil {
		return *new(TxnOk), err
	}

	result := transaction{}
	var primary *int = nil
	for _, op := range req.Txn {
		switch op.Op {
		case "r":
			value, err := read(s.kv, op.Key, startTs)
			if err != nil {
				return *new(TxnOk), err
			}
			result = append(result, NewTxnOp(op.Op, op.Key, value))
		case "w":
			if primary == nil {
				primary = new(int)
				*primary = op.Key
			}
			if err := preWrite(s.kv, op.Key, startTs, *op.Value, *primary); err != nil {
				return *new(TxnOk), err
			}
			result = append(result, NewTxnOp(op.Op, op.Key, op.Value))
		}
	}

	// Commit phase
	commitTs, err := s.tso.Get()
	if err != nil {
		return *new(TxnOk), err
	}

	for _, op := range req.Txn {
		switch op.Op {
		case "w":
			if err := commit(s.kv, op.Key, startTs, commitTs, *primary); err != nil {
				return *new(TxnOk), err
			}
		}
	}

	res := TxnOk{
		Txn: result,
	}
	return res, nil
}

func read(kv *maelstrom.KV, key int, startTs int) (*int, error) {
	keyStr := strconv.Itoa(key)
	cells, err := utils.ReadOrElse(kv, keyStr, []cell{})
	if err != nil {
		return nil, err
	}

	for i := len(cells) - 1; i >= 0; i-- {
		c := cells[i]
		if c.Lock != nil && c.Ts < startTs {
			return nil, errors.New("earlier-started transaction found") // TODO retry
		}
	}

	var dataTs *int = nil
	for i := len(cells) - 1; i >= 0; i-- {
		c := cells[i]
		// from same transaction
		if c.Ts == startTs {
			if c.Lock == nil {
				panic("expected lock")
			}
			return c.Data, nil
		}
		// from earlier transaction
		if c.Write != nil && c.Write.DataTs < startTs {
			dataTs = new(int)
			*dataTs = c.Write.DataTs
			continue
		}
		if dataTs != nil && *dataTs == c.Ts {
			return c.Data, nil
		}
	}
	if dataTs != nil {
		panic("commited data not found")
	}

	return nil, nil
}

func preWrite(kv *maelstrom.KV, key int, startTs int, data int, primary int) error {
	keyStr := strconv.Itoa(key)
	cells, err := utils.ReadOrElse(kv, keyStr, []cell{})
	if err != nil {
		return err
	}
	unmodified, err := utils.DeepCopy(cells)
	if err != nil {
		return err
	}

	override := false
	for i := len(cells) - 1; i >= 0; i-- {
		c := &cells[i]
		if c.Lock != nil && c.Ts > startTs {
			return errors.New("write conflict: " + strconv.Itoa(c.Ts) + " > " + strconv.Itoa(startTs)) // TODO rollback and abortion response ???
		}
		if c.Lock != nil && c.Ts == startTs {
			if *c.Lock != primary {
				panic("primary mismatch")
			}
			c.Data = &data // override previous write in same transaction
			override = true
			break
		}
	}

	if !override {
		cells = append(cells, cell{
			Ts:   startTs,
			Data: &data,
			Lock: &primary,
		})
	}

	return kv.CompareAndSwap(context.Background(), keyStr, unmodified, cells, true) // TODO handle CAS failure, retry ???
}

func commit(kv *maelstrom.KV, key int, startTs int, commitTs int, primary int) error {
	keyStr := strconv.Itoa(key)
	cells, err := utils.ReadOrElse(kv, keyStr, []cell{})
	if err != nil {
		return err
	}
	unmodified, err := utils.DeepCopy(cells)
	if err != nil {
		return err
	}

	// remove lock
	removed := false
	for i := len(cells) - 1; i >= 0; i-- {
		c := &cells[i]
		if c.Ts == startTs && c.Lock != nil {
			if *c.Lock != primary {
				panic("primary mismatch")
			}
			c.Lock = nil
			removed = true
			break
		}
	}

	// mark write (at most once)
	if removed {
		cells = append(cells, cell{
			Ts: commitTs,
			Write: &write{
				DataTs: startTs,
				Kind:   writePut,
			},
		})
	}

	return kv.CompareAndSwap(context.Background(), keyStr, unmodified, cells, true) // TODO handle CAS failure, retry ???
}
