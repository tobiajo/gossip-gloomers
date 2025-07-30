package common

import (
	"encoding/json"
	"testing"
)

func TestTxnOp_MarshalJSON(t *testing.T) {
	val := 42
	tests := []struct {
		name string
		op   TxnOp
		want string
	}{
		{
			name: "with value",
			op:   TxnOp{Op: "r", Key: 1, Value: &val},
			want: `["r",1,42]`,
		},
		{
			name: "nil value",
			op:   TxnOp{Op: "w", Key: 2, Value: nil},
			want: `["w",2,null]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := json.Marshal(tt.op)
			if err != nil {
				t.Fatalf("Marshal failed: %v", err)
			}
			if string(got) != tt.want {
				t.Errorf("Marshal = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestTxnOp_UnmarshalJSON(t *testing.T) {
	val := 99
	tests := []struct {
		name    string
		input   string
		want    TxnOp
		wantErr bool
	}{
		{
			name:  "valid with value",
			input: `["r", 3, 99]`,
			want:  TxnOp{Op: "r", Key: 3, Value: &val},
		},
		{
			name:  "valid with nil value",
			input: `["w", 4, null]`,
			want:  TxnOp{Op: "w", Key: 4, Value: nil},
		},
		{
			name:    "wrong number of elements",
			input:   `["r", 1]`,
			wantErr: true,
		},
		{
			name:    "non-string op",
			input:   `[5, 1, 2]`,
			wantErr: true,
		},
		{
			name:    "non-numeric key",
			input:   `["r", "key", 2]`,
			wantErr: true,
		},
		{
			name:    "non-numeric value",
			input:   `["r", 1, "val"]`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got TxnOp
			err := json.Unmarshal([]byte(tt.input), &got)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Unmarshal error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				if got.Op != tt.want.Op || got.Key != tt.want.Key ||
					(got.Value == nil) != (tt.want.Value == nil) ||
					(got.Value != nil && *got.Value != *tt.want.Value) {
					t.Errorf("Unmarshal = %+v, want %+v", got, tt.want)
				}
			}
		})
	}
}
