package raft

import "errors"

// non-durable - loses everything the second the process exits. good
// enough for tests/harness.go where I want a fresh cluster every run
// and don't care about surviving a restart. for anything that needs
// real crash recovery, see FilePersister in persister_file.go.
type MemoryPersister struct {
	term     uint64
	votedFor PeerID
	log      []LogEntry

	snapshot          []byte
	lastIncludedIndex uint64
	lastIncludedTerm  uint64
	hasSnapshot       bool
}

func NewMemoryPersister() *MemoryPersister {
	return &MemoryPersister{}
}

func (p *MemoryPersister) SaveState(currentTerm uint64, votedFor PeerID, log []LogEntry) error {
	p.term = currentTerm
	p.votedFor = votedFor
	p.log = append([]LogEntry(nil), log...)
	return nil
}

func (p *MemoryPersister) LoadState() (uint64, PeerID, []LogEntry, error) {
	if p.log == nil && p.term == 0 && p.votedFor == "" {
		return 0, "", nil, errors.New("no persisted state")
	}
	return p.term, p.votedFor, p.log, nil
}

func (p *MemoryPersister) SaveSnapshot(data []byte, lastIncludedIndex, lastIncludedTerm uint64) error {
	p.snapshot = data
	p.lastIncludedIndex = lastIncludedIndex
	p.lastIncludedTerm = lastIncludedTerm
	p.hasSnapshot = true
	return nil
}

func (p *MemoryPersister) LoadSnapshot() ([]byte, uint64, uint64, error) {
	if !p.hasSnapshot {
		return nil, 0, 0, errors.New("no snapshot")
	}
	return p.snapshot, p.lastIncludedIndex, p.lastIncludedTerm, nil
}
