package main

import (
	"context"
	"fmt"
	"log"
	"strconv"

	mapset "github.com/deckarep/golang-set/v2"
	. "github.com/tobiajo/gossip-gloomers/common"
	utils "github.com/tobiajo/gossip-gloomers/utils"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	kv  *maelstrom.KV
	tso *utils.TSO
}

type transaction = []TxnOp

type transactionConflict struct {
	message string
}

func (e *transactionConflict) Error() string {
	return fmt.Sprintf("transaction conflict: %s", e.message)
}

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

	readLocks := getReadLocks(req.Txn)
	locks := mapset.NewSet[int]()

	result := transaction{}
	primary := req.Txn[0].Key // TODO improve
	var conflict *transactionConflict

	for _, op := range req.Txn {
		switch op.Op {
		case "r":
			var value *int
			if readLocks.Contains(op.Key) {
				value, err = readWithLock(s.kv, op.Key, startTs, primary)
				if err != nil {
					if e, ok := err.(*transactionConflict); ok {
						conflict = e
						break
					}
					return *new(TxnOk), err
				}
				locks.Add(op.Key)
			} else {
				value, err = read(s.kv, op.Key, startTs)
				if err != nil {
					return *new(TxnOk), err
				}
			}
			result = append(result, NewTxnOp(op.Op, op.Key, value))
		case "w":
			err := preWrite(s.kv, op.Key, startTs, *op.Value, primary)
			if err != nil {
				if e, ok := err.(*transactionConflict); ok {
					conflict = e
					break
				}
				return *new(TxnOk), err
			}
			locks.Add(op.Key)
			result = append(result, NewTxnOp(op.Op, op.Key, op.Value))
		}
	}

	// Commit phase
	commitTs, err := s.tso.Get()
	if err != nil {
		return *new(TxnOk), err
	}

	var kind writeKind
	if conflict != nil {
		kind = writeRollback
	} else {
		kind = writeCommit
	}

	commited := mapset.NewSet[int]()
	for _, op := range req.Txn {
		if op.Op == "w" && locks.Contains(op.Key) && !commited.Contains(op.Key) {
			if err := commit(s.kv, op.Key, startTs, commitTs, primary, kind); err != nil {
				return *new(TxnOk), err
			}
			commited.Add(op.Key)
			locks.Remove(op.Key)
		}
	}

	for key := range locks.Iter() {
		log.Printf("[req=%v] removing read-read lock for key %d", req, key)
		if err := removeReadLock(s.kv, key, startTs, primary); err != nil { // if no commits; read-read transactions
			return *new(TxnOk), err
		}
	}

	if conflict != nil {
		log.Printf("[req=%v] retrying transaction due to conflict", req)
		return s.txnHandler(req)
	}

	res := TxnOk{
		Txn: result,
	}
	return res, nil
}

// TODO use or remove; unused
func getReadsBeforeSubsequentOps(txn []TxnOp) mapset.Set[int] {
	reads := mapset.NewSet[int]()
	writes := mapset.NewSet[int]()
	opCount := make(map[int]int)

	for _, op := range txn {
		opCount[op.Key]++
		if op.Op == "r" && !writes.Contains(op.Key) {
			reads.Add(op.Key)
		} else if op.Op == "w" {
			writes.Add(op.Key)
		}
	}

	filteredReads := mapset.NewSet[int]()
	for key := range reads.Iter() {
		if opCount[key] > 1 {
			filteredReads.Add(key)
		}
	}

	return filteredReads
}

func getReadLocks(txn []TxnOp) mapset.Set[int] {
	reads := mapset.NewSet[int]()
	opCount := make(map[int]int)

	for _, op := range txn {
		opCount[op.Key]++
		if op.Op == "r" {
			reads.Add(op.Key)
		}
	}

	// filteredReads := mapset.NewSet[int]()
	// for key := range reads.Iter() {
	// 	if opCount[key] > 1 {
	// 		filteredReads.Add(key)
	// 	}
	// }

	return reads //filteredReads
}

func readWithLock(kv *maelstrom.KV, key int, startTs int, primary int) (*int, error) {
	keyStr := strconv.Itoa(key)
	cells, err := utils.ReadOrElse(kv, keyStr, []cell{})
	if err != nil {
		return new(int), err
	}

	locked, err := checkLock(cells, startTs, primary)
	if err != nil {
		log.Printf("[key=%d, startTs=%d, primary=%d] read rejection due to %s", key, startTs, primary, err.Error())
		return new(int), err
	}

	data := getLastWrite(cells, startTs)

	if !locked {
		unmodified, err := utils.DeepCopy(cells)
		if err != nil {
			return new(int), err
		}
		log.Printf("[key=%d, startTs=%d, primary=%d] adding read lock", key, startTs, primary)
		cells = append(cells, cell{
			Ts:   startTs,
			Data: data, // dummy write
			Lock: &primary,
		})

		err = kv.CompareAndSwap(context.Background(), keyStr, unmodified, cells, true)
		if err != nil {
			log.Printf("[key=%d, startTs=%d, primary=%d] retrying read with lock due to CAS failure", key, startTs, primary)
			return readWithLock(kv, key, startTs, primary)
		}
	}

	return data, nil
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
			log.Printf("[key=%d, startTs=%d] awaiting transaction at %d", key, startTs, c.Ts)
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
				log.Printf("[key=%d, startTs=%d] proceeding with read", key, startTs)
				earlierTxn = false
			}
		}
	}

	data := getLastWrite(cells, startTs)

	return data, nil
}

func getLastWrite(cells []cell, startTs int) *int {
	var dataTs *int
	var data *int

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

	return data
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

	locked, err := checkLock(cells, startTs, primary)
	if err != nil {
		log.Printf("[key=%d, startTs=%d, primary=%d] write rejection due to %s", key, startTs, primary, err.Error())
		return err
	}

	if locked {
		updateData(cells, startTs, data, primary)
	} else {
		log.Printf("[key=%d, startTs=%d, data=%d, primary=%d] adding write lock", key, startTs, data, primary)
		cells = append(cells, cell{
			Ts:   startTs,
			Data: &data,
			Lock: &primary,
		})
	}

	err = kv.CompareAndSwap(context.Background(), keyStr, unmodified, cells, true)
	if err != nil {
		log.Printf("[key=%d, startTs=%d, data=%d, primary=%d] retrying prepare due to CAS failure", key, startTs, data, primary)
		return preWrite(kv, key, startTs, data, primary)
	}
	return nil
}

func checkLock(cells []cell, startTs int, primary int) (bool, error) {
	for i := len(cells) - 1; i >= 0; i-- {
		c := cells[i]
		if c.Lock != nil {
			if c.Ts == startTs {
				if *c.Lock != primary {
					panic("primary mismatch")
				}
				return true, nil
			} else {
				return *new(bool), &transactionConflict{"other lock at " + strconv.Itoa(c.Ts)}
			}
		}
		// TODO missing conflict where primary commited and secondries not ???
		if c.Ts > startTs && c.Write != nil && c.Write.Kind == writeCommit {
			return *new(bool), &transactionConflict{"newer commit at " + strconv.Itoa(c.Ts)}
		}
	}
	return false, nil
}

func updateData(cells []cell, startTs int, data int, primary int) {
	for i := len(cells) - 1; i >= 0; i-- {
		c := &cells[i]
		if c.Lock != nil && c.Ts == startTs {
			if *c.Lock != primary {
				panic("primary mismatch")
			}
			c.Data = &data // override previous write in same transaction
			return
		}
	}
	panic("lock not found")
}

func commit(kv *maelstrom.KV, key int, startTs int, commitTs int, primary int, kind writeKind) error {
	keyStr := strconv.Itoa(key)
	cells, err := utils.ReadOrElse(kv, keyStr, []cell{})
	if err != nil {
		return err
	}
	unmodified, err := utils.DeepCopy(cells)
	if err != nil {
		return err
	}

	log.Printf("[key=%d, startTs=%d, commitTs=%d, primary=%d, kind=%s] removing lock", key, startTs, commitTs, primary, kind)
	removeLock(cells, startTs, primary)

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

	return nil
}

func removeReadLock(kv *maelstrom.KV, key int, startTs int, primary int) error {
	keyStr := strconv.Itoa(key)
	cells, err := utils.ReadOrElse(kv, keyStr, []cell{})
	if err != nil {
		return err
	}
	unmodified, err := utils.DeepCopy(cells)
	if err != nil {
		return err
	}

	log.Printf("[key=%d, startTs=%d, primary=%d] removing read lock", key, startTs, primary)
	removeLock(cells, startTs, primary)

	err = kv.CompareAndSwap(context.Background(), keyStr, unmodified, cells, true)
	if err != nil {
		log.Printf("[key=%d, startTs=%d, primary=%d] retrying read lock removal due to CAS failure", key, startTs, primary)
		return removeReadLock(kv, key, startTs, primary)
	}

	return nil
}

func removeLock(cells []cell, startTs int, primary int) {
	for i := len(cells) - 1; i >= 0; i-- {
		c := &cells[i]
		if c.Ts == startTs && c.Lock != nil {
			if *c.Lock != primary {
				panic("primary mismatch")
			}
			c.Lock = nil // remove lock
			return
		}
	}
	panic("lock not found")
}
