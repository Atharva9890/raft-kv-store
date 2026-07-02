package raft

// anything durable enough to survive a restart has to go through
// here: currentTerm, votedFor, and the log itself (Figure 2). without
// this a restarted node could vote twice in a term it already voted
// in, or forget entries it told a leader it had - both are exactly
// the kind of bug that only shows up during a real outage.
//
// I've got two implementations: MemoryPersister (persister_memory.go)
// for tests, and FilePersister (persister_file.go) for anything that
// needs to survive a process restart, which is the only one that
// makes the "kill a node, it comes back correctly" demo honest.
type Persister interface {
	SaveState(currentTerm uint64, votedFor PeerID, log []LogEntry) error
	LoadState() (currentTerm uint64, votedFor PeerID, log []LogEntry, err error)

	SaveSnapshot(data []byte, lastIncludedIndex, lastIncludedTerm uint64) error
	LoadSnapshot() (data []byte, lastIncludedIndex, lastIncludedTerm uint64, err error)
}

// small wrapper around the entry slice that hides the index math once
// a snapshot has chopped off the front of the log. entries[0] is not
// necessarily log index 0 or even index 1 - it's whatever comes right
// after lastIncludedIndex. every place that needs to go from "raft
// index" to "slice index" goes through toArrayIndex so I only had to
// get the off-by-one right once.
type log struct {
	entries           []LogEntry
	lastIncludedIndex uint64
	lastIncludedTerm  uint64
}

func newLog() *log {
	return &log{entries: make([]LogEntry, 0)}
}

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

// drops everything from raftIndex onward - used when a leader's
// AppendEntries tells me I've got uncommitted entries that don't
// match what it actually replicated.
func (l *log) truncateFrom(raftIndex uint64) {
	i, ok := l.toArrayIndex(raftIndex)
	if !ok {
		return
	}
	l.entries = l.entries[:i]
}
