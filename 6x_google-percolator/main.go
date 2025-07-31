package main

import (
	"context"
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
	writeCommit   writeKind = "Commit"
	writeRollback writeKind = "Rollback"
)

// https://tikv.org/deep-dive/distributed-transaction/percolator/
// TODO danglig lock clean-up after crashes
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
	// Prepare phase
	startTs, err := s.tso.Get()
	if err != nil {
		return *new(TxnOk), err
	}

	writeConflict := false

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
			conflictFree, err := prepare(s.kv, op.Key, startTs, *op.Value, *primary)
			if err != nil {
				return *new(TxnOk), err
			}
			writeConflict = writeConflict || !conflictFree
			if writeConflict {
				break
			}
			result = append(result, NewTxnOp(op.Op, op.Key, op.Value))
		}
	}

	// Commit phase
	commitTs, err := s.tso.Get()
	if err != nil {
		return *new(TxnOk), err
	}

	var kind writeKind
	if writeConflict {
		kind = writeRollback
	} else {
		kind = writeCommit
	}

	for _, op := range req.Txn {
		switch op.Op {
		case "w":
			if _, err := commit(s.kv, op.Key, startTs, commitTs, *primary, kind); err != nil {
				return *new(TxnOk), err
			}
		}
	}

	if writeConflict {
		log.Default().Printf("[req=%v] aborting transaction due to write conflict", req)
		return *new(TxnOk), &maelstrom.RPCError{Code: maelstrom.TxnConflict, Text: "The requested transaction has been aborted because of a conflict with another transaction. Servers need not return this error on every conflict: they may choose to retry automatically instead."}
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

	earlierTxn := false
	for i := len(cells) - 1; i >= 0; i-- {
		c := cells[i]
		if c.Lock != nil && c.Ts < startTs {
			log.Default().Printf("[key=%d, startTs=%d] awaiting transaction at %d", key, startTs, c.Ts)
			earlierTxn = true
			break
		}
	}
	// retry until no earlier on-going transactions
	for earlierTxn {
		cells, err = utils.ReadOrElse(kv, keyStr, []cell{})
		if err != nil {
			return nil, err
		}
		for i := len(cells) - 1; i >= 0; i-- {
			c := cells[i]
			if c.Lock != nil && c.Ts < startTs {
				break
			}
			if i == 0 {
				log.Default().Printf("[key=%d, startTs=%d] proceeding with read", key, startTs)
				earlierTxn = false
			}
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
		if c.Write != nil && c.Write.Kind == writeCommit && c.Write.DataTs < startTs {
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

func prepare(kv *maelstrom.KV, key int, startTs int, data int, primary int) (bool, error) {
	keyStr := strconv.Itoa(key)
	cells, err := utils.ReadOrElse(kv, keyStr, []cell{})
	if err != nil {
		return *new(bool), err
	}
	unmodified, err := utils.DeepCopy(cells)
	if err != nil {
		return *new(bool), err
	}

	override := false
	for i := len(cells) - 1; i >= 0; i-- {
		c := &cells[i]
		if c.Lock != nil && c.Ts > startTs {
			log.Default().Printf("[key=%d, startTs=%d, data=%d, primary=%d] newer lock at %d", key, startTs, data, primary, c.Ts)
			return false, nil
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

	err = kv.CompareAndSwap(context.Background(), keyStr, unmodified, cells, true)
	if err != nil {
		log.Default().Printf("[key=%d, startTs=%d, data=%d, primary=%d] retrying prepare due to CAS failure", key, startTs, data, primary)
		return prepare(kv, key, startTs, data, primary)
	}
	return true, nil
}

func commit(kv *maelstrom.KV, key int, startTs int, commitTs int, primary int, kind writeKind) (bool, error) {
	keyStr := strconv.Itoa(key)
	cells, err := utils.ReadOrElse(kv, keyStr, []cell{})
	if err != nil {
		return *new(bool), err
	}
	unmodified, err := utils.DeepCopy(cells)
	if err != nil {
		return *new(bool), err
	}

	removed := false
	for i := len(cells) - 1; i >= 0; i-- {
		c := &cells[i]
		if c.Ts == startTs && c.Lock != nil {
			if *c.Lock != primary {
				panic("primary mismatch")
			}
			c.Lock = nil // remove lock
			removed = true
			break
		}
	}

	if !removed {
		return false, nil // already committed
	}

	if kind == writeRollback {
		log.Default().Printf("[key=%d, startTs=%d, commitTs=%d, primary=%d, kind=%s] rolling back", key, startTs, commitTs, primary, kind)
	}
	cells = append(cells, cell{ // mark write
		Ts: commitTs,
		Write: &write{
			DataTs: startTs,
			Kind:   kind,
		},
	})

	err = kv.CompareAndSwap(context.Background(), keyStr, unmodified, cells, true)
	if err != nil {
		log.Default().Printf("[key=%d, startTs=%d, commitTs=%d, primary=%d, kind=%s] retrying commit due to CAS failure", key, startTs, commitTs, primary, kind)
		return commit(kv, key, startTs, commitTs, primary, kind)
	}

	return true, nil
}
