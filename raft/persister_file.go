package raft

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

// writes state/snapshot to disk as plain JSON. not the fastest thing
// I could've built (no fsync tuning, no binary format, rewrites the
// whole file on every save instead of appending) but it's honest -
// kill -9 a node mid-write and the worst case is you lose the last
// write, not the whole file, since I write to a temp file and rename
// over the real one instead of writing in place.
type FilePersister struct {
	mu           sync.Mutex
	stateFile    string
	snapshotFile string
}

func NewFilePersister(dir string) (*FilePersister, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &FilePersister{
		stateFile:    filepath.Join(dir, "state.json"),
		snapshotFile: filepath.Join(dir, "snapshot.json"),
	}, nil
}

type persistedState struct {
	CurrentTerm uint64     `json:"current_term"`
	VotedFor    PeerID     `json:"voted_for"`
	Log         []LogEntry `json:"log"`
}

func (p *FilePersister) SaveState(currentTerm uint64, votedFor PeerID, log []LogEntry) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return writeJSONAtomic(p.stateFile, persistedState{
		CurrentTerm: currentTerm,
		VotedFor:    votedFor,
		Log:         log,
	})
}

func (p *FilePersister) LoadState() (uint64, PeerID, []LogEntry, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var s persistedState
	if err := readJSON(p.stateFile, &s); err != nil {
		return 0, "", nil, err
	}
	return s.CurrentTerm, s.VotedFor, s.Log, nil
}

type persistedSnapshot struct {
	Data              []byte `json:"data"`
	LastIncludedIndex uint64 `json:"last_included_index"`
	LastIncludedTerm  uint64 `json:"last_included_term"`
}

func (p *FilePersister) SaveSnapshot(data []byte, lastIncludedIndex, lastIncludedTerm uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return writeJSONAtomic(p.snapshotFile, persistedSnapshot{
		Data:              data,
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
	})
}

func (p *FilePersister) LoadSnapshot() ([]byte, uint64, uint64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var s persistedSnapshot
	if err := readJSON(p.snapshotFile, &s); err != nil {
		return nil, 0, 0, err
	}
	return s.Data, s.LastIncludedIndex, s.LastIncludedTerm, nil
}

func writeJSONAtomic(path string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func readJSON(path string, v any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}
