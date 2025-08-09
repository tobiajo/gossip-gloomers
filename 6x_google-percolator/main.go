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

const (
	keepVersions   = 1    // MVCC
	lockSingleRead = true // false causes G2-item anomalies, i.e. fractured reads, which violates snapshot-isolation
)

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
	Lock  *int   `json:"lock"` // primary key of the transaction that locked this cell
	Write *write `json:"write"`
}

type write struct {
	DataTs int       `json:"data_ts"`
	Kind   writeKind `json:"kind"`
}

type writeKind string

const (
	writeCommit   writeKind = "commit"
	writeRollback writeKind = "rollback"
)

// https://tikv.org/deep-dive/distributed-transaction/percolator/
// TODO clean up dangling locks after crashes
func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)
	tso := utils.NewLinTSO(n)
	s := server{
		kv:  kv,
		tso: tso,
	}

	utils.RegisterHandlerWithContext(n, "txn", s.txnHandler)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func (s *server) txnHandler(ctx context.Context, req Txn) (TxnOk, error) {
	// Prepare phase
	startTs, err := s.tso.Get()
	if err != nil {
		return *new(TxnOk), err
	}

	readLocks := getReadLocks(req.Txn, lockSingleRead)
	placedLocks := mapset.NewSet[int]()

	result := transaction{}
	var primary *int
	var conflict *transactionConflict

	for _, op := range req.Txn {
		switch op.Op {
		case "r":
			var value *int
			if readLocks.Contains(op.Key) {
				if primary == nil {
					primary = &op.Key
				}
				value, err = readWithLock(s.kv, ctx, op.Key, startTs, *primary)
				if err != nil {
					if e, ok := err.(*transactionConflict); ok {
						conflict = e
						break
					}
					return *new(TxnOk), err
				}
				placedLocks.Add(op.Key)
			} else {
				value, err = read(s.kv, ctx, op.Key, startTs)
				if err != nil {
					return *new(TxnOk), err
				}
			}
			result = append(result, NewTxnOp(op.Op, op.Key, value))
		case "w":
			if primary == nil {
				primary = &op.Key
			}
			err := preWrite(s.kv, ctx, op.Key, startTs, *op.Value, *primary)
			if err != nil {
				if e, ok := err.(*transactionConflict); ok {
					conflict = e
					break
				}
				return *new(TxnOk), err
			}
			placedLocks.Add(op.Key)
			result = append(result, NewTxnOp(op.Op, op.Key, op.Value))
		}
	}

	// Commit phase
	// TODO https://tikv.org/deep-dive/distributed-transaction/optimized-percolator/#calculated-commit-timestamp without read lock
	commitTs, err := s.tso.Get()
	if err != nil {
		return *new(TxnOk), err
	}

	var kind writeKind
	if conflict == nil {
		kind = writeCommit
	} else {
		kind = writeRollback
	}

	for _, op := range req.Txn {
		if op.Op == "w" && placedLocks.Contains(op.Key) {
			if err := commit(s.kv, ctx, op.Key, startTs, commitTs, *primary, kind); err != nil {
				return *new(TxnOk), err
			}
			placedLocks.Remove(op.Key)
		}
	}
	for key := range placedLocks.Iter() {
		if err := releaseReadOnlyLock(s.kv, ctx, key, startTs, *primary); err != nil { // if no commits
			return *new(TxnOk), err
		}
	}

	if conflict != nil {
		log.Printf("[msgId=%s, req=%s] retrying transaction due to conflict", ctx.Value(utils.MsgIdKey), req)
		return s.txnHandler(ctx, req)
	}

	res := TxnOk{
		Txn: result,
	}
	return res, nil
}

func getReadLocks(txn []TxnOp, lockSingleRead bool) mapset.Set[int] {
	reads := mapset.NewSet[int]()
	opCount := make(map[int]int)

	for _, op := range txn {
		opCount[op.Key]++
		if op.Op == "r" {
			reads.Add(op.Key)
		}
	}

	if lockSingleRead {
		return reads
	} else {
		filtered := mapset.NewSet[int]()
		for key := range reads.Iter() {
			if opCount[key] > 1 {
				filtered.Add(key)
			}
		}
		return filtered
	}
}

func readWithLock(kv *maelstrom.KV, ctx context.Context, key int, startTs int, primary int) (*int, error) {
	keyStr := strconv.Itoa(key)
	cells, err := utils.ReadOrElse(kv, keyStr, []cell{})
	if err != nil {
		return new(int), err
	}

	locked, err := checkLock(cells, startTs, primary)
	if err != nil {
		log.Printf("[msgId=%s, key=%d, startTs=%d, primary=%d] read rejection due to %s", ctx.Value(utils.MsgIdKey), key, startTs, primary, err)
		return new(int), err
	}

	data := getLastWrite(cells, startTs)

	if !locked {
		unmodified, err := utils.DeepCopy(cells)
		if err != nil {
			return new(int), err
		}
		log.Printf("[msgId=%s, key=%d, startTs=%d, primary=%d] placing read lock", ctx.Value(utils.MsgIdKey), key, startTs, primary)
		cells = append(cells, cell{
			Ts:   startTs,
			Data: data, // write existing value
			Lock: &primary,
		})

		err = kv.CompareAndSwap(context.Background(), keyStr, unmodified, cells, true)
		if err != nil {
			log.Printf("[msgId=%s, key=%d, startTs=%d, primary=%d] retrying read with lock due to CAS failure", ctx.Value(utils.MsgIdKey), key, startTs, primary)
			return readWithLock(kv, ctx, key, startTs, primary)
		}
	}

	return data, nil
}

func read(kv *maelstrom.KV, ctx context.Context, key int, startTs int) (*int, error) {
	keyStr := strconv.Itoa(key)
	cells, err := utils.ReadOrElse(kv, keyStr, []cell{})
	if err != nil {
		return nil, err
	}

	earlierTxn := false
	for i := len(cells) - 1; i >= 0; i-- {
		c := cells[i]
		if c.Lock != nil && c.Ts < startTs {
			log.Printf("[msgId=%s, key=%d, startTs=%d] awaiting transaction at %d", ctx.Value(utils.MsgIdKey), key, startTs, c.Ts)
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
				log.Printf("[msgId=%s, key=%d, startTs=%d] proceeding with read", ctx.Value(utils.MsgIdKey), key, startTs)
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

func preWrite(kv *maelstrom.KV, ctx context.Context, key int, startTs int, data int, primary int) error {
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
		log.Printf("[msgId=%s, key=%d, startTs=%d, primary=%d] write rejection due to %s", ctx.Value(utils.MsgIdKey), key, startTs, primary, err)
		return err
	}

	if locked {
		updateData(cells, startTs, data, primary)
	} else {
		log.Printf("[msgId=%s, key=%d, startTs=%d, data=%d, primary=%d] placing write lock", ctx.Value(utils.MsgIdKey), key, startTs, data, primary)
		cells = append(cells, cell{
			Ts:   startTs,
			Data: &data,
			Lock: &primary,
		})
	}

	err = kv.CompareAndSwap(context.Background(), keyStr, unmodified, cells, true)
	if err != nil {
		log.Printf("[msgId=%s, key=%d, startTs=%d, data=%d, primary=%d] retrying prepare due to CAS failure", ctx.Value(utils.MsgIdKey), key, startTs, data, primary)
		return preWrite(kv, ctx, key, startTs, data, primary)
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
	c := &cells[len(cells)-1]
	if c.Lock != nil && c.Ts == startTs {
		if *c.Lock != primary {
			panic("primary mismatch")
		}
		c.Data = &data // override previous write in same transaction
		return
	} else {
		panic("lock not found")
	}
}

func commit(kv *maelstrom.KV, ctx context.Context, key int, startTs int, commitTs int, primary int, kind writeKind) error {
	keyStr := strconv.Itoa(key)
	cells, err := utils.ReadOrElse(kv, keyStr, []cell{})
	if err != nil {
		return err
	}
	unmodified, err := utils.DeepCopy(cells)
	if err != nil {
		return err
	}

	log.Printf("[msgId=%s, key=%d, startTs=%d, commitTs=%d, primary=%d, kind=%s] releasing lock", ctx.Value(utils.MsgIdKey), key, startTs, commitTs, primary, kind)
	releaseLock(cells, startTs, primary)

	cells = append(cells, cell{ // mark write
		Ts: commitTs,
		Write: &write{
			DataTs: startTs,
			Kind:   kind,
		},
	})

	switch kind {
	case writeCommit:
		keepCells := keepVersions * 2
		if len(cells) > keepCells {
			cells = cells[len(cells)-keepCells:] // truncate history
		}
	case writeRollback:
		log.Printf("[msgId=%s, key=%d, startTs=%d, commitTs=%d, primary=%d, kind=%s] rolling back", ctx.Value(utils.MsgIdKey), key, startTs, commitTs, primary, kind)
		cells = cells[:len(cells)-2]
	}

	err = kv.CompareAndSwap(context.Background(), keyStr, unmodified, cells, true)
	if err != nil {
		log.Printf("[msgId=%s, key=%d, startTs=%d, commitTs=%d, primary=%d, kind=%s] retrying commit due to CAS failure", ctx.Value(utils.MsgIdKey), key, startTs, commitTs, primary, kind)
		return commit(kv, ctx, key, startTs, commitTs, primary, kind)
	}

	return nil
}

func releaseReadOnlyLock(kv *maelstrom.KV, ctx context.Context, key int, startTs int, primary int) error {
	keyStr := strconv.Itoa(key)
	cells, err := utils.ReadOrElse(kv, keyStr, []cell{})
	if err != nil {
		return err
	}
	unmodified, err := utils.DeepCopy(cells)
	if err != nil {
		return err
	}

	log.Printf("[msgId=%s, key=%d, startTs=%d, primary=%d] releasing read-only lock", ctx.Value(utils.MsgIdKey), key, startTs, primary)
	releaseLock(cells, startTs, primary)

	cells = cells[:len(cells)-1] // dropping redundant cell

	err = kv.CompareAndSwap(context.Background(), keyStr, unmodified, cells, true)
	if err != nil {
		log.Printf("[msgId=%s, key=%d, startTs=%d, primary=%d] retrying read-only lock release due to CAS failure", ctx.Value(utils.MsgIdKey), key, startTs, primary)
		return releaseReadOnlyLock(kv, ctx, key, startTs, primary)
	}

	return nil
}

func releaseLock(cells []cell, startTs int, primary int) {
	c := &cells[len(cells)-1]
	if c.Ts == startTs && c.Lock != nil {
		if *c.Lock != primary {
			panic("primary mismatch")
		}
		c.Lock = nil // release lock
		return
	} else {
		panic("lock not found")
	}
}
