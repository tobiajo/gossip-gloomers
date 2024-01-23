package main

type Add struct {
	Delta int `json:"delta"`
}

type AddOk struct{}

type Read struct{}

type ReadOk struct {
	Value int `json:"value"`
}
