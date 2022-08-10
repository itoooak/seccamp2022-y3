package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"sc.y3/dispatcher"
	"sc.y3/peer"
)

const (
	HEARTBEAT_INTERVAL       = 1
	HEARTBEAT_WAITLIMIT_BASE = 5
	VOTE_WAITLIMIT           = 5
)

func main() {
	host, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	nameFlag := flag.String("name", host, "Worker name")
	portFlag := flag.Int("port", 30000, "Port number")
	delayFlag := flag.Int("delay", 0, "Communication delay")
	dispatcherFlag := flag.String("dispatcher", "", "Dispathcer address")
	flag.Parse()

	addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", host, *portFlag))
	if err != nil {
		log.Fatal(err)
	}

	log.Println(addr.String())

	node := peer.NewNode(addr.String(), peer.Delay(*delayFlag))
	dispatcher, err := dispatcher.FindDispatcher(*dispatcherFlag)
	if err != nil {
		log.Fatal(err)
	}

	worker := peer.NewWorker(*nameFlag)
	if node.LinkWorker(worker) != nil {
		log.Fatal(err)
	}

	peers, err := dispatcher.GetConnectedPeers(worker)
	if err != nil {
		log.Fatal(err)
	}
	for name, addr := range peers {
		if name != worker.Name() {
			worker.Connect(name, addr)
		}
	}

	log.Printf("start")

	for {
		switch worker.State.State {
		case peer.Leader:
			leader(worker)
		case peer.Follower:
			follower(worker)
		case peer.Candidate:
			candidate(worker)
		default:
			log.Fatal("invalid state")
		}
	}
}

func leader(w *peer.Worker) {
	currentTerm := w.State.Term
	// 最初にHeartbeat
	log.Printf("heartbeat in term %d", currentTerm)
	for dest, _ := range w.ConnectedPeers() {
		var reply peer.RequestHeartbeatReply
		err := w.RemoteCall(dest, "Worker.RequestHeartbeat",
			peer.RequestHeartbeatArgs{From: w.Name(), Term: w.State.Term}, &reply)
		if err != nil {
			log.Fatal(err)
		}
	}

	for w.State.State == peer.Leader {
		heartbeatClock := time.After(HEARTBEAT_INTERVAL * time.Second)
		select {
		case m := <-w.Channels.Heartbeat:
			if w.State.Term < m.Term {
				w.LockMutex()
				w.State.State = peer.Follower
				w.State.Leader = m.From
				w.State.Term = m.Term
				w.UnlockMutex()
				log.Printf("listen heartbeat from newer leader %s in term %d", w.State.Leader, w.State.Term)
				log.Printf("follow %s in term %d", w.State.Leader, w.State.Term)
				return
			}
		case <-heartbeatClock:
			log.Printf("heartbeat in term %d", currentTerm)
			for dest, _ := range w.ConnectedPeers() {
				var reply peer.RequestHeartbeatReply
				err := w.RemoteCall(dest, "Worker.RequestHeartbeat",
					peer.RequestHeartbeatArgs{From: w.Name(), Term: w.State.Term}, &reply)
				if err != nil {
					log.Fatal(err)
				}
			}
		}
	}
}

func follower(w *peer.Worker) {
	HeartbeatWaitLimit := time.Duration(w.Rand().ExpFloat64()/HEARTBEAT_WAITLIMIT_BASE) * time.Second
	w.LockMutex()
	currentTerm := w.State.Term
	currentLeader := w.State.Leader
	w.UnlockMutex()

	if w.State.Voted[currentTerm] {
		for w.State.Term == currentTerm && w.State.Leader == currentLeader {
			select {
			case m := <-w.Channels.Heartbeat:
				if w.State.Term < m.Term {
					w.LockMutex()
					w.State.Term = m.Term
					w.State.Leader = m.From
					w.UnlockMutex()
					log.Printf("follow %s in term %d", w.State.Leader, w.State.Term)
					return
				} else if w.State.Term == m.Term && w.State.Leader != m.From {
					w.LockMutex()
					w.State.Leader = m.From
					w.UnlockMutex()
					log.Printf("follow %s in term %d", w.State.Leader, w.State.Term)
					return
				}
			}
		}
		return
	}

	for !w.State.Voted[currentTerm] {
		select {
		// TODO: この中のロジックが間違っていそう
		case m := <-w.Channels.Heartbeat:
			if w.State.Term == m.Term {
				break
			} else if w.State.Term < m.Term {
				w.LockMutex()
				w.State.Term = m.Term
				w.State.Leader = m.From
				w.UnlockMutex()
				log.Printf("follow %s in term %d", w.State.Leader, w.State.Term)
				return
			}
		case <-time.After(HeartbeatWaitLimit * time.Second):
			log.Printf("no heartbeat")
			w.State.State = peer.Candidate
			return
		}
	}
}

func candidate(w *peer.Worker) {
	w.LockMutex()
	w.State.Term += 1
	currentTerm := w.State.Term
	w.State.Voted[currentTerm] = true
	log.Printf("vote %s in term %d", w.Name(), currentTerm)
	w.UnlockMutex()

	nodeNum := 1
	approvalCounter := struct {
		sync.Mutex
		counter int
	}{
		Mutex:   sync.Mutex{},
		counter: 1,
	}

	for dest, _ := range w.ConnectedPeers() {
		go func(dest string) {
			log.Printf("request %s to vote in term %d", dest, currentTerm)
			var reply peer.RequestVoteReply
			err := w.RemoteCall(dest, "Worker.RequestVote",
				peer.RequestVoteArgs{From: w.Name(), Term: currentTerm}, &reply)
			if err != nil {
				log.Fatal(err)
			}
			if reply.Message.Approve {
				approvalCounter.Lock()
				approvalCounter.counter += 1
				approvalCounter.Unlock()
				log.Printf("voted by %s in term %d", dest, currentTerm)
			} else {
				log.Printf("refused by %s in term %d", dest, currentTerm)
			}
			log.Printf("%#v", reply)
		}(dest)
		nodeNum += 1
	}

	voteWaitClock := time.After(VOTE_WAITLIMIT * time.Second)

	for {
		empty := false
		select {
		// 自分より新しいLeaderが存在するならfollow
		case m := <-w.Channels.Heartbeat:
			if currentTerm <= m.Term {
				w.LockMutex()
				w.State.State = peer.Follower
				w.State.Term = m.Term
				w.State.Leader = m.From
				w.UnlockMutex()
				log.Printf("follow %s in term %d", w.State.Leader, w.State.Term)
				return
			}
		default:
			empty = true
		}

		if empty {
			break
		}
	}

	select {
	case <-voteWaitClock:
		approvalCounter.Lock()
		w.LockMutex()

		if approvalCounter.counter*2 > nodeNum && w.State.Term == currentTerm {
			w.State.State = peer.Leader
			log.Printf("accepted (%d / %d)", approvalCounter.counter, nodeNum)
			log.Printf("leader in term %d", currentTerm)
		}

		approvalCounter.Unlock()
		w.UnlockMutex()
	}

	return
}
