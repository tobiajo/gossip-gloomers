package main

import (
	"context"
	"log"
	utils "github.com/tobiajo/gossip-gloomers/utils"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	kv *maelstrom.KV
}

func messageLogKey(key string) string {
	return "messageLog," + key
}

func committedOffsetKey(key string) string {
	return "committedOffset," + key
}

// Challenge #5b: Multi-Node Kafka-Style Log
// https://fly.io/dist-sys/5b
func main() {
	n := maelstrom.NewNode()
	s := server{
		kv: maelstrom.NewLinKV(n),
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
	messageLog, err := utils.ReadOrElse(s.kv, messageLogKey(req.Key), []int{})
	if err != nil {
		return *new(SendOk), err
	}
	offset := len(messageLog)
	updatedMessageLog := append(messageLog, req.Msg)
	err = s.kv.CompareAndSwap(context.Background(), messageLogKey(req.Key), messageLog, updatedMessageLog, true)
	if err != nil {
		return *new(SendOk), err
	}

	res := SendOk{
		Offset: offset,
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
		for i, message := range messageLog[offset:] {
			pairs = append(pairs, [2]int{offset + i, message})
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
