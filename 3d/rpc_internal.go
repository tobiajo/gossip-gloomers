package main

type Deliver struct {
	Messages []int `json:"messages"`
}

type DeliverOk struct {
	Src      string `json:"src"`
	Messages []int  `json:"message"`
}
