package main

type Send struct {
	Key string `json:"key"`
	Msg int    `json:"msg"`
}

type SendOk struct {
	Offset int `json:"offset"`
}

type Poll struct {
	Offsets map[string]int `json:"offsets"`
}

type PollOk struct {
	Msgs map[string][][2]int `json:"msgs"`
}

type CommitOffsets struct {
	Offsets map[string]int `json:"offsets"`
}

type CommitOffsetsOk struct{}

type ListCommittedOffsets struct {
	Keys []string `json:"keys"`
}

type ListCommittedOffsetsOk struct {
	Offsets map[string]int `json:"offsets"`
}
