package common

import (
	"encoding/json"
	"fmt"
)

type TxnOp struct {
	Op    string
	Key   int
	Value *int
}

func NewTxnOp(op string, key int, value *int) TxnOp {
	return TxnOp{
		Op:    op,
		Key:   key,
		Value: value,
	}
}

func (op *TxnOp) UnmarshalJSON(data []byte) error {
	var raw []any
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	if len(raw) != 3 {
		return fmt.Errorf("expected 3 elements in txn op, got %d", len(raw))
	}

	// First: operation
	if opStr, ok := raw[0].(string); ok {
		op.Op = opStr
	} else {
		return fmt.Errorf("invalid op type")
	}

	// Second: key
	if keyFloat, ok := raw[1].(float64); ok {
		op.Key = int(keyFloat)
	} else {
		return fmt.Errorf("invalid key type")
	}

	// Third: value (can be null)
	if raw[2] == nil {
		op.Value = nil
	} else if valFloat, ok := raw[2].(float64); ok {
		val := int(valFloat)
		op.Value = &val
	} else {
		return fmt.Errorf("invalid value type")
	}

	return nil
}

func (op TxnOp) MarshalJSON() ([]byte, error) {
	var val any
	if op.Value != nil {
		val = *op.Value
	} else {
		val = nil
	}
	return json.Marshal([]any{op.Op, op.Key, val})
}
