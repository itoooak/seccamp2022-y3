package peer

import (
	"fmt"
	"log"

	"github.com/oklog/ulid/v2"
)

type WorkerValueDiff struct {
	Id       ulid.ULID
	Operator StateUpdateOperator
	Operand  int
}

type NodeState int

const (
	Leader    = 0
	Follower  = 1
	Candidate = 2
)

type WorkerState struct {
	Diffs  []WorkerValueDiff
	Leader string
	Term   uint
	State  NodeState
	Voted  map[uint]bool
	commitIndex uint

	// leaderのみ使用
	// nextIndex: 各followerが次に受け取る(はずである)index
	nextIndex map[string]uint
	// matchIndex
	matchIndex map[string]uint
}

func InitState(w *Worker) WorkerState {
	return WorkerState{
		Diffs:  []WorkerValueDiff{},
		Leader: w.name,
		Term:   0,
		State:  Follower,
		Voted:  map[uint]bool{},
		nextIndex: make(map[string]uint),
		matchIndex: make(map[string]uint),
	}
}

func InitChannels(w *Worker) ConnChannels {
	return ConnChannels{
		Heartbeat: make(chan HeartbeatMessage, 10),
		Vote:      make(chan VoteMessage, 10),
	}
}

func CalcValue(s *WorkerState) int {
	value := 0
	for i, d := range s.Diffs {
		if i > int(s.commitIndex) {
			break
		}
		switch d.Operator {
		case "ADD":
			value += d.Operand
		case "MUL":
			value *= d.Operand
		}
	}
	return value
}

func (s *WorkerState) String() string {
	return fmt.Sprintf("Value: %d", CalcValue(s))
}

type RequestStateArgs struct{}

type RequestStateReply struct {
	State WorkerState
}

func (w *Worker) RequestState(args RequestStateArgs, reply *RequestStateReply) error {
	w.LockMutex()
	reply.State = w.State
	w.UnlockMutex()

	log.Println(reply.State.String())

	return nil
}
