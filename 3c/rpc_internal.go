package main

type Deliver struct {
	Message int `json:"message"`
}

type DeliverOk struct {
	Src     string `json:"src"`
	Message int    `json:"message"`
}
