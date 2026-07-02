package server

import (
	"encoding/json"
	"sync"
)

type OpType string

const (
	OpGet    OpType = "get"
	OpPut    OpType = "put"
	OpDelete OpType = "delete"
)

// Op is the command every KV operation is encoded into before being
// proposed to Raft. Routing Get through the log too (instead of
// reading local state directly) is what makes reads linearizable: a
// Get can't return a stale value just because it happened to be
// served by a node that hasn't caught up yet, because it has to wait
// its turn in the same log every write goes through.
type Op struct {
	Type  OpType `json:"type"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

func EncodeOp(op Op) ([]byte, error) { return json.Marshal(op) }

func DecodeOp(data []byte) (Op, error) {
	var op Op
	err := json.Unmarshal(data, &op)
	return op, err
}

// KVStore is the replicated state machine. Every node's KVStore ends
// up with identical contents because raft/raft.go's applyLoop feeds
// every node the exact same sequence of committed entries, and Apply
// is a pure function of (current state, op).
type KVStore struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewKVStore() *KVStore {
	return &KVStore{data: make(map[string]string)}
}

// Apply mutates the store per op and returns the value relevant to a
// Get (ignored for Put/Delete).
func (s *KVStore) Apply(op Op) (value string, found bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch op.Type {
	case OpGet:
		value, found = s.data[op.Key]
	case OpPut:
		s.data[op.Key] = op.Value
	case OpDelete:
		delete(s.data, op.Key)
	}
	return value, found
}

// Snapshot serializes the entire store, for raft.Node.TakeSnapshot
// once that TODO is implemented.
func (s *KVStore) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(s.data)
}

func (s *KVStore) Restore(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	fresh := make(map[string]string)
	if err := json.Unmarshal(data, &fresh); err != nil {
		return err
	}
	s.data = fresh
	return nil
}
