package main

type Txn struct {
	Txn [][3]any `json:"txn"`
}

type TxnOk struct {
	Txn [][3]any `json:"txn"`
}
