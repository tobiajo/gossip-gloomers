package main

import (
	"log"
	utils "github.com/tobiajo/gossip-gloomers/utils"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	messageLogs           map[string][]message
	committedOffsets      map[string]int
	messageLogsMutex      sync.Mutex
	committedOffsetsMutex sync.Mutex
}

type message = int

// Challenge #5a: Single-Node Kafka-Style Log
// https://fly.io/dist-sys/5a
func main() {
	n := maelstrom.NewNode()
	s := server{
		messageLogs:      make(map[string][]message),
		committedOffsets: make(map[string]int),
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
	s.messageLogsMutex.Lock()
	defer s.messageLogsMutex.Unlock()
	messageLog, ok := s.messageLogs[req.Key]
	if !ok {
		messageLog = []message{}
	}
	offset := len(messageLog)
	messageLog = append(messageLog, req.Msg)
	s.messageLogs[req.Key] = messageLog

	res := SendOk{
		Offset: offset,
	}
	return res, nil
}

func (s *server) pollHandler(req Poll) (PollOk, error) {
	msgs := make(map[string][][2]int)
	for key, offset := range req.Offsets {
		s.messageLogsMutex.Lock()
		messageLog := s.messageLogs[key][offset:]
		s.messageLogsMutex.Unlock()
		var pairs [][2]int
		for i, message := range messageLog {
			pair := [2]int{offset + i, message}
			pairs = append(pairs, pair)
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
		s.committedOffsetsMutex.Lock()
		s.committedOffsets[key] = offset
		s.committedOffsetsMutex.Unlock()
	}

	res := CommitOffsetsOk{}
	return res, nil
}

func (s *server) listCommittedOffsetsHandler(req ListCommittedOffsets) (ListCommittedOffsetsOk, error) {
	offsets := make(map[string]int)
	for _, key := range req.Keys {
		s.committedOffsetsMutex.Lock()
		offsets[key] = s.committedOffsets[key]
		s.committedOffsetsMutex.Unlock()
	}

	res := ListCommittedOffsetsOk{
		Offsets: offsets,
	}
	return res, nil
}
