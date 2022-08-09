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
	HEARTBEAT_WAITLIMIT_BASE = 3
	VOTE_WAITLIMIT           = 2
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
			panic("invalid state")
		}
	}
}

func leader(w *peer.Worker) {
	for {
		select {
		case m := <-w.Channels.Heartbeat:
			if w.State.Term < m.Term {
				w.State.State = peer.Follower
				w.State.Leader = m.From
				w.State.Term = m.Term
				log.Printf("follow %s in term %d", w.State.Leader, w.State.Term)
				return
			}
		case <-time.After(HEARTBEAT_INTERVAL * time.Second):
			for dest, _ := range w.ConnectedPeers() {
				var reply peer.RequestHeartbeatReply
				w.RemoteCall(dest, "Worker.RequestHeartbeat",
					peer.RequestHeartbeatArgs{From: w.Name(), Term: w.State.Term}, &reply)
			}
		}
	}
}

func follower(w *peer.Worker) {
	HeartbeatWaitLimit := time.Duration(w.Rand().ExpFloat64()/HEARTBEAT_WAITLIMIT_BASE) * time.Second
	for {
		select {
		case <-w.Channels.Heartbeat:
			return
		case <-time.After(HeartbeatWaitLimit * time.Second):
			w.State.State = peer.Candidate
			return
		}
	}
}

func candidate(w *peer.Worker) {
	w.State.Term += 1
	nodeNum := 1
	approvalCounter := struct {
		sync.Mutex
		counter int
	}{
		Mutex:   sync.Mutex{},
		counter: 1,
	}

	for dest, _ := range w.ConnectedPeers() {
		var reply peer.RequestVoteReply
		go func() {
			w.RemoteCall(dest, "Worker.RequestVote",
				peer.RequestVoteArgs{From: w.Name(), Term: w.State.Term}, &reply)
			approvalCounter.Lock()
			defer approvalCounter.Unlock()
			approvalCounter.counter += 1
		}()
		nodeNum += 1
	}

	select {
	case <-time.After(VOTE_WAITLIMIT):
		approvalCounter.Lock()

		if approvalCounter.counter*2 > nodeNum {
			w.State.State = peer.Leader
			log.Printf("leader in term %d", w.State.Term)
		}

		approvalCounter.Unlock()
	}

	return
}
