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
	// Millisecond
	HEARTBEAT_INTERVAL       = 1000
	HEARTBEAT_WAITLIMIT_BASE = 5000
	VOTE_WAITLIMIT           = 5000
)

func main() {
	host, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	nameFlag := flag.String("name", host, "Worker name")
	portFlag := flag.Int("port", 30000, "Port number")
	delayFlag := flag.Int("delay", 0, "Communication delay")
	dispatcherFlag := flag.String("dispatcher", "", "Dispatcher address")
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
	heartbeat(w, currentTerm)

	for w.State.State == peer.Leader {
		heartbeatClock := time.After(HEARTBEAT_INTERVAL * time.Millisecond)
		select {
		case m := <-w.Channels.Heartbeat:
			if currentTerm < m.Term {
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
			heartbeat(w, currentTerm)
		}
	}

	w.LockMutex()
	defer w.UnlockMutex()
	log.Printf("follow %s in term %d", w.State.Leader, w.State.Term)
}

func heartbeat(w *peer.Worker, currentTerm uint) {
	log.Printf("heartbeat in term %d", currentTerm)
	for dest, _ := range w.ConnectedPeers() {
		w.SendUpdatingMessageToFollower(dest)
	}
}

func follower(w *peer.Worker) {
	HeartbeatWaitLimit := time.Duration(w.Rand().ExpFloat64()/HEARTBEAT_WAITLIMIT_BASE) * time.Millisecond
	w.LockMutex()
	currentTerm := w.State.Term
	currentLeader := w.State.Leader
	w.UnlockMutex()

	if w.State.Voted[currentTerm] {
		for w.State.Term == currentTerm && w.State.Leader == currentLeader {
			leaderSelectionWaitLimit := HeartbeatWaitLimit + VOTE_WAITLIMIT*time.Millisecond
			leaderSelectionWaitClock := time.After(leaderSelectionWaitLimit)
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
				} else if m.From == currentLeader && m.Term == currentTerm {
					log.Printf("following %s in term %d", currentLeader, currentTerm)
				}
			case <-leaderSelectionWaitClock:
				log.Printf("no heartbeat")
				w.LockMutex()
				defer w.UnlockMutex()
				w.State.State = peer.Candidate
				return
			}
		}
		return
	}

	if !w.State.Voted[currentTerm] {
		for w.State.Term == currentTerm && w.State.Leader == currentLeader {
			select {
			case m := <-w.Channels.Heartbeat:
				if currentTerm == m.Term && currentLeader == m.From {
					log.Printf("following %s in term %d", currentLeader, currentTerm)
					break
				} else if currentLeader != m.From || currentTerm < m.Term {
					w.LockMutex()
					w.State.Term = m.Term
					w.State.Leader = m.From
					w.UnlockMutex()
					log.Printf("follow %s in term %d", w.State.Leader, w.State.Term)
					return
				}
			case <-time.After(HeartbeatWaitLimit * time.Millisecond):
				log.Printf("no heartbeat")
				w.State.State = peer.Candidate
				return
			}
		}
	}
}

type counter struct {
	sync.Mutex
	counter int
}

func newCounter() counter {
	return counter{
		Mutex:   sync.Mutex{},
		counter: 1,
	}
}

func candidate(w *peer.Worker) {
	w.LockMutex()
	w.State.Term += 1
	currentTerm := w.State.Term
	w.State.Voted[currentTerm] = true
	w.State.Leader = w.Name()
	log.Printf("vote %s in term %d", w.Name(), currentTerm)
	w.UnlockMutex()

	nodeNum := newCounter()
	approvalCounter := newCounter()

	for dest, _ := range w.ConnectedPeers() {
		go func(dest string) {
			log.Printf("request %s to vote in term %d", dest, currentTerm)
			var reply peer.RequestVoteReply
			err := w.RemoteCall(dest, "Worker.RequestVote",
				peer.RequestVoteArgs{From: w.Name(), Term: currentTerm}, &reply)
			if err != nil {
				log.Printf("%s (%s)", err.Error(), dest)
				w.Disconnect(dest)
			} else {
				nodeNum.Lock()
				nodeNum.counter += 1
				nodeNum.Unlock()
			}
			if reply.Message.Approve {
				approvalCounter.Lock()
				approvalCounter.counter += 1
				approvalCounter.Unlock()
				log.Printf("voted by %s in term %d", dest, currentTerm)
			} else {
				log.Printf("refused by %s in term %d", dest, currentTerm)
			}
			// log.Printf("%#v", reply)
		}(dest)
	}

	voteWaitClock := time.After(VOTE_WAITLIMIT * time.Millisecond)

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

		if approvalCounter.counter*2 > nodeNum.counter && w.State.Term == currentTerm {
			w.State.State = peer.Leader
			log.Printf("accepted (%d / %d)", approvalCounter.counter, nodeNum.counter)
			log.Printf("leader in term %d", currentTerm)
		}

		approvalCounter.Unlock()
		w.UnlockMutex()
	}

	return
}
