package raft

type State uint8

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

type MessageType uint8

const (
	MsgRequestVote MessageType = iota
	MsgRequestVoteResponse
	MsgAppend
	MsgAppendResponse
	MsgPropose
	MsgHeartbeat
	MsgHeartbeatResponse
)

type Entry struct {
	Index uint64
	Term  uint64
	Data  []byte
}

type HardState struct {
	Term   uint64
	Vote   uint64
	Commit uint64
}

type Message struct {
	Type       MessageType
	From       uint64
	To         uint64
	Term       uint64
	LogTerm    uint64
	Index      uint64
	Commit     uint64
	Entries    []Entry
	Reject     bool
	RejectHint uint64
	Context    []byte
}

type Ready struct {
	HardState HardState
	Entries   []Entry
	Messages  []Message
	Committed []Entry
}
