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

	result := transaction{}
	readCache := make(map[int]*int) // TODO replace with read lock to avoid G2 anomolies
	var primary *int = nil
	writeConflict := false

	for _, op := range req.Txn {
		switch op.Op {
		case "r":
			value, ok := readCache[op.Key]
			if !ok {
				value, err = read(s.kv, op.Key, startTs)
				readCache[op.Key] = value
				if err != nil {
					return *new(TxnOk), err
				}
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
			readCache[op.Key] = op.Value
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
		log.Printf("[req=%v] rolling back transaction due to write conflict", req)
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
		return *new(TxnOk), maelstrom.NewRPCError(maelstrom.TxnConflict, "write conflict")
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

	earlierTx := false
	for i := len(cells) - 1; i >= 0; i-- {
		c := cells[i]
		if c.Lock != nil && c.Ts < startTs {
			log.Printf("[key=%d, startTs=%d] awaiting transaction at %d", key, startTs, c.Ts)
			earlierTx = true
			break
		}
	}
	// retry until no earlier on-going transactions
	for earlierTx {
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
				log.Printf("[key=%d, startTs=%d] proceeding with read", key, startTs)
				earlierTx = false
			}
		}
	}

	var dataTs *int = nil
	var data *int = nil

	for i := len(cells) - 1; i >= 0; i-- {
		c := cells[i]

		// from same transaction
		if c.Ts == startTs {
			if c.Lock == nil {
				panic("expected lock")
			}
			data = c.Data
			break
		}

		// from earlier transaction
		// TODO should only check if primary commited ???
		if c.Write != nil && c.Write.Kind == writeCommit && c.Ts < startTs {
			dataTs = new(int)
			*dataTs = c.Write.DataTs
			continue
		}
		if dataTs != nil && *dataTs == c.Ts {
			data = c.Data
			break
		}
	}
	if dataTs != nil && data == nil {
		panic("commited data not found")
	}

	return data, nil
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

	overridden := false
	for i := len(cells) - 1; i >= 0; i-- {
		c := &cells[i]
		if c.Lock != nil {
			if c.Ts == startTs {
				if *c.Lock != primary {
					panic("primary mismatch")
				}
				c.Data = &data // override previous write in same transaction
				overridden = true
				break
			} else {
				log.Printf("[key=%d, startTs=%d, data=%d, primary=%d] conflicting lock at %d", key, startTs, data, primary, c.Ts)
				return false, nil
			}
		}
		// TODO missing conflict where primary commited and secondries not ???
		if c.Ts > startTs && c.Write != nil && c.Write.Kind == writeCommit {
			log.Printf("[key=%d, startTs=%d, data=%d, primary=%d] newer commit at %d", key, startTs, data, primary, c.Ts)
			return false, nil
		}
	}

	if !overridden {
		cells = append(cells, cell{
			Ts:   startTs,
			Data: &data,
			Lock: &primary,
		})
	}

	err = kv.CompareAndSwap(context.Background(), keyStr, unmodified, cells, true)
	if err != nil {
		log.Printf("[key=%d, startTs=%d, data=%d, primary=%d] retrying prepare due to CAS failure", key, startTs, data, primary)
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

	if removed {
		if kind == writeRollback {
			log.Printf("[key=%d, startTs=%d, commitTs=%d, primary=%d, kind=%s] rolling back", key, startTs, commitTs, primary, kind)
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
			log.Printf("[key=%d, startTs=%d, commitTs=%d, primary=%d, kind=%s] retrying commit due to CAS failure", key, startTs, commitTs, primary, kind)
			return commit(kv, key, startTs, commitTs, primary, kind)
		}

		return true, nil
	} else {
		return false, nil // already committed
	}
}
