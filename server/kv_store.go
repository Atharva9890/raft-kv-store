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

// every KV operation gets encoded into one of these before I propose
// it to Raft. yes, that includes Get - routing reads through the log
// too is what makes them linearizable, since a Get can't come back
// stale just because it landed on a node that hasn't caught up, it
// has to wait its turn in the same ordering everything else does.
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

// the replicated state machine. every node's copy ends up identical
// because raft/raft.go's applyLoop feeds each one the exact same
// sequence of committed entries, and Apply is a pure function of
// (current state, op) - nothing here reads the clock or anything else
// that could make two nodes diverge on the same input.
type KVStore struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewKVStore() *KVStore {
	return &KVStore{data: make(map[string]string)}
}

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

// dumps the whole store so raft.Node.TakeSnapshot can compact the log
// past whatever this represents.
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
