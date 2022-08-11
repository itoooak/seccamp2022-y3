package peer

import (
	"fmt"
	"log"
	"math/rand"
	"sync"

	"github.com/oklog/ulid/v2"
)

type Worker struct {
	name string
	node *Node

	mu sync.Mutex

	Channels ConnChannels
	State    WorkerState
}

type ConnChannels struct {
	Heartbeat chan HeartbeatMessage
	Vote      chan VoteMessage
	Update    chan UpdatingMessage
}

type HeartbeatMessage struct {
	From string
	To   string
	Term uint
}

type VoteMessage struct {
	Approve bool
	From    string
	To      string
	Term    uint
}

type UpdatingMessage struct {
	DiffEntries  []WorkerValueDiff
	prevLogIndex uint
	leaderCommit uint

	// OK chan<- bool
	From string
	Term uint
}

type WorkerOption func(*Worker)

func NewWorker(name string) *Worker {
	w := new(Worker)
	w.name = name
	w.State = InitState(w)
	w.Channels = InitChannels(w)
	return w
}

func (w *Worker) Name() string {
	return w.name
}

func (w *Worker) Addr() string {
	return w.node.Addr()
}

func (w *Worker) LockMutex() {
	w.mu.Lock()
}

func (w *Worker) UnlockMutex() {
	w.mu.Unlock()
}

func (w *Worker) Rand() *rand.Rand {
	return w.node.Rand()
}

func (w *Worker) LinkNode(n *Node) {
	w.node = n
}

func (w *Worker) Connect(name, addr string) (err error) {
	w.LockMutex()
	defer w.UnlockMutex()
	err = w.node.Connect(name, addr)
	if err != nil {
		return err
	}
	var reply RequestConnectReply
	err = w.RemoteCall(name, "Worker.RequestConnect", RequestConnectArgs{w.name, w.node.Addr()}, &reply)
	if err != nil {
		return err
	} else if !reply.OK {
		return fmt.Errorf("Connection request denied: [%s] %s", name, addr)
	}
	w.State.nextIndex[name] = 1
	w.State.matchIndex[name] = 0
	// for n, a := range reply.Peers {
	// 	if !w.node.IsConnectedTo(n) {
	// 		err = w.node.Connect(n, a)
	// 		if err != nil {
	// 			return err
	// 		}
	// 	}
	// }
	return nil
}

func (w *Worker) Disconnect(name string) (err error) {
	w.LockMutex()
	defer w.UnlockMutex()
	err = w.node.Disconnect(name)
	if err != nil {
		return err
	}
	delete(w.State.matchIndex, name)
	delete(w.State.nextIndex, name)
	return nil
}

func (w *Worker) Stop() {
	w.node.Shutdown()
	w.node = nil
}

func (w *Worker) RemoteCall(name, method string, args any, reply any) error {
	return w.node.call(name, method, args, reply)
}

func (w *Worker) ConnectedPeers() map[string]string {
	return w.node.ConnectedNodes()
}

type RequestConnectArgs struct {
	Name string
	Addr string
}

type RequestConnectReply struct {
	OK    bool
	Peers map[string]string
}

func (w *Worker) RequestConnect(args RequestConnectArgs, reply *RequestConnectReply) error {
	reply.OK = false
	reply.Peers = make(map[string]string)
	w.LockMutex()
	defer w.UnlockMutex()
	err := w.node.Connect(args.Name, args.Addr)
	if err != nil {
		return err
	}
	w.State.nextIndex[args.Name] = 1
	w.State.matchIndex[args.Name] = 0
	reply.OK = true
	for name, addr := range w.node.ConnectedNodes() {
		reply.Peers[name] = addr
	}
	return nil
}

type RequestConnectedPeersArgs struct{}

type RequestConnectedPeersReply struct {
	Peers map[string]string
}

func (w *Worker) RequestConnectedPeers(args RequestConnectedPeersArgs, reply *RequestConnectedPeersReply) error {
	w.LockMutex()
	defer w.UnlockMutex()
	reply.Peers = make(map[string]string)
	for k, v := range w.ConnectedPeers() {
		reply.Peers[k] = v
	}
	return nil
}

type StateUpdateOperator string

const (
	Add = StateUpdateOperator("ADD")
	Mul = StateUpdateOperator("MUL")
)

type RequestStateUpdateArgs struct {
	Id       ulid.ULID
	Operator StateUpdateOperator
	Operand  int
}

type RequestStateUpdateReply struct {
	OK     bool
	Before int
	After  int
}

func (w *Worker) RequestStateUpdate(args RequestStateUpdateArgs, reply *RequestStateUpdateReply) error {
	w.LockMutex()
	for _, d := range w.State.Diffs {
		if d.Id == args.Id {
			return nil
		}
	}

	reply.Before = CalcValue(&w.State)

	if w.State.State == Leader {
		switch args.Operator {
		case "ADD":
			w.State.Diffs = append(w.State.Diffs, WorkerValueDiff{Id: args.Id, Operator: "ADD", Operand: args.Operand})
		case "MUL":
			w.State.Diffs = append(w.State.Diffs, WorkerValueDiff{Id: args.Id, Operator: "MUL", Operand: args.Operand})
		default:
			reply.OK = false
			return fmt.Errorf("unknown operator")
		}
	}

	reply.After = CalcValue(&w.State)
	reply.OK = true

	w.UnlockMutex()

	log.Printf("%#v\n", reply)

	if w.State.State == Leader {
		var wg sync.WaitGroup
		peers := w.ConnectedPeers()

		for peer := range peers {
			wg.Add(1)
			go func(peer string) {
				log.Printf("connect %s: request update", peer)
				ok, err := w.sendUpdatingMessageToFollower(peer)
				for !ok {
					if err != nil {
						log.Printf("failed to send updating message to %s", peer)
						w.Disconnect(peer)
						wg.Done()
						return
					}

					w.LockMutex()
					w.State.nextIndex[peer] -= 1
					w.UnlockMutex()
					ok, err = w.sendUpdatingMessageToFollower(peer)
				}

				w.LockMutex()
				w.State.nextIndex[peer] = uint(len(w.State.Diffs))
				w.State.matchIndex[peer] = uint(len(w.State.Diffs) - 1)
				w.UnlockMutex()
				wg.Done()
			}(peer)
		}
		wg.Wait()

		for k := uint(len(w.State.Diffs) - 1); k > w.State.commitIndex; k-- {
			nodeNum := 1
			updatedNum := 1
			for peer := range w.ConnectedPeers() {
				nodeNum += 1
				if w.State.matchIndex[peer] >= k {
					updatedNum += 1
				}
			}

			if updatedNum*2 > nodeNum {
				w.LockMutex()
				w.State.commitIndex = k
				w.UnlockMutex()
				return nil
			}
		}
	} else if w.State.State == Follower {
		leader := w.State.Leader
		var r RequestStateUpdateReply
		log.Printf("connect %s (leader): request update", leader)
		err := w.RemoteCall(leader, "Worker.RequestStateUpdate", args, &r)
		if err != nil {
			log.Printf("failed to call %s: %s", leader, err.Error())
			return err
		}
	}

	return nil
}

func (w *Worker) sendUpdatingMessageToFollower(name string) (bool, error) {
	var reply RequestReceiveUpdatingMessageReply
	w.LockMutex()
	msg := UpdatingMessage{
		DiffEntries:  w.State.Diffs[w.State.nextIndex[name]:],
		prevLogIndex: w.State.nextIndex[name] - 1,
		leaderCommit: w.State.commitIndex,
		From:         w.name,
		Term:         w.State.Term,
	}
	w.UnlockMutex()

	err := w.RemoteCall(name, "Worker.RequestReceiveUpdatingMessage", RequestReceiveUpdatingMessageArgs{Msg: msg}, &reply)
	if err != nil {
		log.Printf("failed in (*Worker).sendUpdatingMessageToFollower: %s", err.Error())
		return false, err
	}

	return reply.OK, nil
}

type RequestReceiveUpdatingMessageArgs struct {
	Msg UpdatingMessage
}

type RequestReceiveUpdatingMessageReply struct {
	OK bool
}

func (w *Worker) RequestReceiveUpdatingMessage(args RequestReceiveUpdatingMessageArgs, reply *RequestReceiveUpdatingMessageReply) (err error) {
	w.LockMutex()
	defer w.UnlockMutex()

	log.Printf("requested to update by %s: %#v", args.Msg.From, args.Msg)

	w.Channels.Heartbeat <- HeartbeatMessage{From: args.Msg.From, To: w.name, Term: args.Msg.Term}

	if uint(len(w.State.Diffs)) <= args.Msg.prevLogIndex {
		reply.OK = false
		return
	}

	w.State.Diffs = append(w.State.Diffs[:args.Msg.prevLogIndex+1], args.Msg.DiffEntries...)
	if args.Msg.leaderCommit > w.State.commitIndex {
		// leaderCommitが1以上であり、Diffsも1以上の長さを持つ
		if args.Msg.leaderCommit < uint(len(w.State.Diffs)-1) {
			w.State.commitIndex = args.Msg.leaderCommit
		} else {
			w.State.commitIndex = uint(len(w.State.Diffs) - 1)
		}
	}

	reply.OK = true

	return nil
}

func (w *Worker) RequestStateUpdateWithoutSync(args RequestStateUpdateArgs, reply *RequestStateUpdateReply) error {
	w.LockMutex()
	defer w.UnlockMutex()

	for _, d := range w.State.Diffs {
		if d.Id == args.Id {
			return nil
		}
	}

	reply.Before = CalcValue(&w.State)

	switch args.Operator {
	case "ADD":
		w.State.Diffs = append(w.State.Diffs, WorkerValueDiff{Id: args.Id, Operator: "ADD", Operand: args.Operand})
	case "MUL":
		w.State.Diffs = append(w.State.Diffs, WorkerValueDiff{Id: args.Id, Operator: "MUL", Operand: args.Operand})
	default:
		reply.OK = false
		return fmt.Errorf("unknown operator")
	}

	reply.After = CalcValue(&w.State)
	reply.OK = true

	log.Printf("%#v\n", reply)

	return nil
}

type RequestDiffsArgs struct{}

type RequestDiffsReply struct {
	Diffs []WorkerValueDiff
}

func (w *Worker) RequestDiffs(args RequestDiffsArgs, reply *RequestDiffsReply) error {
	w.LockMutex()
	defer w.UnlockMutex()

	reply.Diffs = w.State.Diffs

	log.Printf("%#v\n", reply)

	return nil
}

type RequestLeaderArgs struct{}

type RequestLeaderReply struct {
	Leader string
}

func (w *Worker) RequestLeader(args RequestLeaderArgs, reply *RequestLeaderReply) error {
	w.LockMutex()
	defer w.UnlockMutex()

	reply.Leader = w.State.Leader
	return nil
}

type RequestHeartbeatArgs struct {
	From string
	Term uint
}

type RequestHeartbeatReply struct{}

func (w *Worker) RequestHeartbeat(args RequestHeartbeatArgs, reply *RequestHeartbeatReply) error {
	w.LockMutex()
	defer w.UnlockMutex()

	w.Channels.Heartbeat <- HeartbeatMessage{From: args.From, To: w.name, Term: args.Term}
	return nil
}

type RequestVoteArgs struct {
	From string
	Term uint
}

type RequestVoteReply struct {
	Message VoteMessage
}

func (w *Worker) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	log.Printf("requested to vote by %s in term %d", args.From, args.Term)
	w.LockMutex()
	currentTerm := w.State.Term
	voted := w.State.Voted[args.Term]
	w.UnlockMutex()

	if voted || currentTerm > args.Term {
		*reply = RequestVoteReply{VoteMessage{Approve: false, From: w.name, To: args.From, Term: args.Term}}
		if voted {
			log.Printf("refuse to vote %s in term %d (already voted)", args.From, args.Term)
		} else if currentTerm > args.Term {
			log.Printf("refuse to vote %s in term %d (request is old: current term is %d)", args.From, args.Term, currentTerm)
		}
		return nil
	}

	w.LockMutex()
	w.State.State = Follower
	w.State.Leader = args.From
	w.State.Term = args.Term
	log.Printf("vote %s in term %d", w.State.Leader, w.State.Term)
	w.State.Voted[args.Term] = true
	w.UnlockMutex()

	*reply = RequestVoteReply{VoteMessage{Approve: true, From: w.name, To: args.From, Term: args.Term}}
	return nil
}
