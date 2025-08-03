package main

import (
	"testing"

	mapset "github.com/deckarep/golang-set/v2"
	. "github.com/tobiajo/gossip-gloomers/common"
)

func TestGetReadsBeforeSubsequentOps(t *testing.T) {
	tests := []struct {
		name     string
		txn      []TxnOp
		expected mapset.Set[int]
	}{
		{
			name: "Single read",
			txn: []TxnOp{
				NewTxnOp("r", 1, nil),
			},
			expected: mapset.NewSet[int](),
		},
		{
			name: "Reads on same key",
			txn: []TxnOp{
				NewTxnOp("r", 1, nil),
				NewTxnOp("r", 1, nil),
			},
			expected: mapset.NewSet(1),
		},
		{
			name: "Reads on different keys",
			txn: []TxnOp{
				NewTxnOp("r", 1, nil),
				NewTxnOp("r", 2, nil),
			},
			expected: mapset.NewSet[int](),
		},
		{
			name: "Read before write",
			txn: []TxnOp{
				NewTxnOp("r", 1, nil),
				NewTxnOp("w", 1, intPtr(10)),
			},
			expected: mapset.NewSet(1),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getReadsBeforeSubsequentOps(tt.txn)
			if !result.Equal(tt.expected) {
				t.Errorf("getReadsBeforeSubsequentOps() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// intPtr returns a pointer to an int
func intPtr(val int) *int {
	return &val
}
