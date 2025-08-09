package main

type Broadcast struct {
	Message int `json:"message"`
}

func (b *Broadcast) ToDeliver() Deliver {
	return Deliver{
		Message: b.Message,
	}
}

type BroadcastOk struct{}

type Read struct{}

type ReadOk struct {
	Messages []int `json:"messages"`
}

type Topology struct {
	Topology map[string][]string `json:"topology"`
}

type TopologyOk struct{}
