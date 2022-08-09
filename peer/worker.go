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

	State WorkerState
}

type WorkerOption func(*Worker)

func NewWorker(name string) *Worker {
	w := new(Worker)
	w.name = name
	w.State = InitState(w)
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
	OK bool
	Peers    map[string]string
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
	reply.OK = true
	for name, addr := range w.node.ConnectedNodes() {
        reply.Peers[name] = addr
	}
	return nil
}

type RequestConnectedPeersArgs struct {}

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
	Id ulid.ULID
	Operator StateUpdateOperator
	Operand int
}

type RequestStateUpdateReply struct {
	OK bool
	Before int
	After int
}

/*
func (w *Worker) RequestStateUpdate(args RequestStateUpdateArgs, reply *RequestStateUpdateReply) error {
	w.LockMutex()
	for _, d := range w.State.Diffs {
		if d.Id == args.Id {
			return nil
		}
	}

	reply.Before = CalcValue(&w.State)
	
	switch args.Operator {
	case "ADD":
		w.State.Diffs = append(w.State.Diffs, WorkerStateDiff{Id: args.Id, PeerName: w.name, Operator: "ADD", Operand: args.Operand})
	case "MUL":
		w.State.Diffs = append(w.State.Diffs, WorkerStateDiff{Id: args.Id, PeerName: w.name, Operator: "MUL", Operand: args.Operand})
	default:
		reply.OK = false
		return fmt.Errorf("unknown operator")
	}

	reply.After = CalcValue(&w.State)
	reply.OK = true

	w.UnlockMutex()

	log.Printf("%#v\n", reply)

	var r *RequestStateUpdateReply
	peers := w.ConnectedPeers()
	for peer := range peers {
		log.Printf("connect %s from %s", peer, w.name)
		w.RemoteCall(peer, "Worker.RequestStateUpdate", args, r)
	}

	return nil
}
*/

func (w *Worker) RequestStateUpdate(args RequestStateUpdateArgs, reply *RequestStateUpdateReply) error {
	w.LockMutex()
	for _, d := range w.State.Diffs {
		if d.Id == args.Id {
			return nil
		}
	}

	reply.Before = CalcValue(&w.State)
	
	switch args.Operator {
	case "ADD":
		w.State.Diffs = append(w.State.Diffs, WorkerStateDiff{Id: args.Id, Operator: "ADD", Operand: args.Operand})
	case "MUL":
		w.State.Diffs = append(w.State.Diffs, WorkerStateDiff{Id: args.Id, Operator: "MUL", Operand: args.Operand})
	default:
		reply.OK = false
		return fmt.Errorf("unknown operator")
	}

	reply.After = CalcValue(&w.State)
	reply.OK = true

	w.UnlockMutex()

	log.Printf("%#v\n", reply)

	var r *RequestStateUpdateReply
	var wg sync.WaitGroup
	peers := w.ConnectedPeers()
	for peer := range peers {
		log.Printf("connect %s", peer)
		wg.Add(1)
		go func(peer string) {
			w.RemoteCall(peer, "Worker.requestStateUpdateWithoutSync", args, r)
			wg.Done()
		}(peer)
	}
	wg.Wait()

	return nil
}

func (w *Worker)requestStateUpdateWithoutSync(args RequestStateUpdateArgs, reply *RequestStateUpdateReply) error {
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
		w.State.Diffs = append(w.State.Diffs, WorkerStateDiff{Id: args.Id, Operator: "ADD", Operand: args.Operand})
	case "MUL":
		w.State.Diffs = append(w.State.Diffs, WorkerStateDiff{Id: args.Id, Operator: "MUL", Operand: args.Operand})
	default:
		reply.OK = false
		return fmt.Errorf("unknown operator")
	}

	reply.After = CalcValue(&w.State)
	reply.OK = true

	log.Printf("%#v\n", reply)

	return nil
}

type RequestDiffsArgs struct {}

type RequestDiffsReply struct {
	Diffs []WorkerStateDiff
}

func (w *Worker) RequestDiffs(args RequestDiffsArgs, reply *RequestDiffsReply) error {
	w.LockMutex()
	defer w.UnlockMutex()

	reply.Diffs = w.State.Diffs

	log.Printf("%#v\n", reply)

	return nil
}