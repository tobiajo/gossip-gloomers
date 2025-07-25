package main

import (
	"context"
	"hash/fnv"
	"log"
	utils "github.com/tobiajo/gossip-gloomers/utils"
	"strconv"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	cmap "github.com/orcaman/concurrent-map/v2"
)

type server struct {
	n   *maelstrom.Node
	kv  *maelstrom.KV
	mus cmap.ConcurrentMap[string, *sync.Mutex]
}

type nodeID = string

func messageLogKey(key string) string {
	return "messageLog," + key
}

func committedOffsetKey(key string) string {
	return "committedOffset," + key
}

func keyToNodeID(key string, nodes int) string {
	hash := fnv.New32()
	hash.Write([]byte(key))
	return "n" + strconv.Itoa(int(hash.Sum32()%uint32(nodes-1))) // TODO: consistent hashing
}

// Challenge #5c: Efficient Kafka-Style Log
// https://fly.io/dist-sys/5c
func main() {
	n := maelstrom.NewNode()
	s := server{
		n:   n,
		kv:  maelstrom.NewSeqKV(n), // TODO: `SeqKV` safe with single writer? assuming eventual consistency reads on non-writer nodes
		mus: cmap.New[*sync.Mutex](),
	}

	utils.RegisterHandler(n, "send", s.sendHandler)
	utils.RegisterHandler(n, "poll", s.pollHandler)
	utils.RegisterHandler(n, "commit_offsets", s.commitOffsetsHandler)
	utils.RegisterHandler(n, "list_committed_offsets", s.listCommittedOffsetsHandler)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func (s *server) sendHandler(req Send) (SendOk, error) {
	key := messageLogKey(req.Key)
	dest := keyToNodeID(key, len(s.n.NodeIDs()))
	if dest != s.n.ID() {
		return utils.Send[Send, SendOk](s.n, "send", dest, req)
	}

	s.mus.SetIfAbsent(key, new(sync.Mutex))
	mu, _ := s.mus.Get(key)
	mu.Lock()
	defer mu.Unlock()
	messageLog, err := utils.ReadOrElse(s.kv, key, []int{})
	if err != nil {
		return *new(SendOk), err
	}
	updatedMessageLog := append(messageLog, req.Msg)
	err = s.kv.Write(context.Background(), key, updatedMessageLog)
	if err != nil {
		return *new(SendOk), err
	}

	res := SendOk{
		Offset: len(updatedMessageLog) - 1,
	}
	return res, nil
}

func (s *server) pollHandler(req Poll) (PollOk, error) {
	msgs := make(map[string][][2]int)
	for key, offset := range req.Offsets {
		messageLog, err := utils.ReadOrElse(s.kv, messageLogKey(key), []int{})
		if err != nil {
			return *new(PollOk), err
		}
		var pairs [][2]int
		if len(messageLog) > offset {
			for i, message := range messageLog[offset:] {
				pairs = append(pairs, [2]int{offset + i, message})
			}
		}
		msgs[key] = pairs
	}

	res := PollOk{
		Msgs: msgs,
	}
	return res, nil
}

func (s *server) commitOffsetsHandler(req CommitOffsets) (CommitOffsetsOk, error) {
	for key, offset := range req.Offsets {
		err := s.kv.Write(context.Background(), committedOffsetKey(key), offset)
		if err != nil {
			return *new(CommitOffsetsOk), err
		}
	}

	res := CommitOffsetsOk{}
	return res, nil
}

func (s *server) listCommittedOffsetsHandler(req ListCommittedOffsets) (ListCommittedOffsetsOk, error) {
	offsets := make(map[string]int)
	for _, key := range req.Keys {
		committedOffset, err := utils.ReadOrElse(s.kv, committedOffsetKey(key), 0)
		if err != nil {
			return *new(ListCommittedOffsetsOk), err
		}
		offsets[key] = committedOffset
	}

	res := ListCommittedOffsetsOk{
		Offsets: offsets,
	}
	return res, nil
}
