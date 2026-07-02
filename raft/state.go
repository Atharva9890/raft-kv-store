package raft

// Persister abstracts durable storage for the fields Raft must
// survive a crash/restart with (Raft §5.1, Figure 2): currentTerm,
// votedFor, and the log. Without this, a restarted node could vote
// twice in the same term or forget committed entries.
//
// TODO(persistence): the in-memory implementation below (see
// NewMemoryPersister in raft.go) loses everything on process exit,
// which is fine for tests but defeats the point of a durable KV
// store. Swap in a real implementation (e.g. an append-only file with
// fsync, or BoltDB/SQLite) before the "kill a node, watch it recover
// its term/vote/log correctly" demo is honest.
type Persister interface {
	SaveState(currentTerm uint64, votedFor PeerID, log []LogEntry) error
	LoadState() (currentTerm uint64, votedFor PeerID, log []LogEntry, err error)

	SaveSnapshot(data []byte, lastIncludedIndex, lastIncludedTerm uint64) error
	LoadSnapshot() (data []byte, lastIncludedIndex, lastIncludedTerm uint64, err error)
}

// log wraps the entry slice with helpers that account for entries
// having been compacted away by a snapshot (index 0 of the slice is
// not necessarily log index 0 - see snapshot.go). Kept deliberately
// small: the interesting invariants belong in log.go, this just
// avoids off-by-one bugs being copy-pasted everywhere.
type log struct {
	entries           []LogEntry
	lastIncludedIndex uint64
	lastIncludedTerm  uint64
}

func newLog() *log {
	return &log{entries: make([]LogEntry, 0)}
}

// toArrayIndex converts a Raft log index into an index into l.entries,
// accounting for compaction. Returns (idx, true) if the entry is
// still held in memory, or (0, false) if it was already snapshotted
// away.
func (l *log) toArrayIndex(raftIndex uint64) (int, bool) {
	if raftIndex <= l.lastIncludedIndex {
		return 0, false
	}
	i := int(raftIndex - l.lastIncludedIndex - 1)
	if i < 0 || i >= len(l.entries) {
		return 0, false
	}
	return i, true
}

func (l *log) lastIndex() uint64 {
	if len(l.entries) == 0 {
		return l.lastIncludedIndex
	}
	return l.entries[len(l.entries)-1].Index
}

func (l *log) lastTerm() uint64 {
	if len(l.entries) == 0 {
		return l.lastIncludedTerm
	}
	return l.entries[len(l.entries)-1].Term
}

func (l *log) at(raftIndex uint64) (LogEntry, bool) {
	i, ok := l.toArrayIndex(raftIndex)
	if !ok {
		return LogEntry{}, false
	}
	return l.entries[i], true
}

func (l *log) append(e LogEntry) {
	l.entries = append(l.entries, e)
}

// truncateFrom drops all entries at or after raftIndex, used when a
// leader's AppendEntries reveals that this node has diverging
// (uncommitted) entries that must be discarded. See log.go TODO.
func (l *log) truncateFrom(raftIndex uint64) {
	i, ok := l.toArrayIndex(raftIndex)
	if !ok {
		return
	}
	l.entries = l.entries[:i]
}
