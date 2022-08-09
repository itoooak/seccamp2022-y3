package peer

import (
	"fmt"
	"log"

	"github.com/oklog/ulid/v2"
)

type WorkerStateDiff struct {
	Id ulid.ULID
	Operator StateUpdateOperator
	Operand int
}

type WorkerState struct {
	Diffs []WorkerStateDiff
}

func InitState(w *Worker) WorkerState {
	return WorkerState{}
}

func CalcValue(s *WorkerState) int {
	value := 0
	for _, d := range s.Diffs {
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
