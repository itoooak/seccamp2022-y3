package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"strings"

	"github.com/oklog/ulid/v2"
	"sc.y3/dispatcher"
	"sc.y3/peer"
	//"github.com/rivo/tview"
)

var (
	dispat *dispatcher.Client
)

func main() {
	dispatcherFlag := flag.String("dispatcher", "localhost:8080", "Dispatcher address")
	flag.Parse()

	var err error
	dispat, err = dispatcher.FindDispatcher(*dispatcherFlag)
	if err != nil {
		log.Fatal(err)
	}

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("? ")
		scanner.Scan()
		input := scanner.Text()
		command := parse(input)
		result, err := command.Exec()
		if err != nil {
			fmt.Printf("> [ERRPR] %s\n", err)
		} else {
			fmt.Println("> ", result)
		}
	}
}

func parse(raw string) Command {
	ret := Command{"", make([]string, 0)}
	for i, v := range strings.Split(raw, " ") {
		switch i {
		case 0:
			ret.operation = v
		default:
			ret.args = append(ret.args, v)
		}
	}
	return ret
}

func send_rpc(peer, method string, args any, reply any) error {
	addr, err := dispat.GetAddr(peer)
	if err != nil {
		return err
	}
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		return err
	}

	return client.Call(method, args, reply)
}

type Command struct {
	operation string
	args      []string
}

func (c *Command) Exec() (string, error) {
	switch c.operation {
	case "state":
		return State(c.args[0])
	case "listpeer":
		return ListPeer(c.args[0])
	case "stateupdate":
		return StateUpdate(c.args[0], c.args[1], c.args[2])
	case "listdiff":
		return ListDiff(c.args[0])
	case "leader":
		return Leader(c.args[0])
	default:
		return "", fmt.Errorf("No such command")
	}
}

func State(name string) (string, error) {
	var reply peer.RequestStateReply
	err := send_rpc(name, "Worker.RequestState", peer.RequestStateArgs{}, &reply)

	// log.Printf("%#v", reply)

	if err != nil {
		return "", err
	}
	return reply.State.String(), nil
}

func ListPeer(name string) (string, error) {
	var reply peer.RequestConnectedPeersReply
	err := send_rpc(name, "Worker.RequestConnectedPeers", peer.RequestStateArgs{}, &reply)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%#v", reply.Peers), nil
}

func StateUpdate(name string, operator string, operand string) (string, error) {
	var reply peer.RequestStateUpdateReply
	operandInt, err := strconv.Atoi(operand)
	if err != nil {
		return "", err
	}

	err2 := send_rpc(name, "Worker.RequestStateUpdate",
		peer.RequestStateUpdateArgs{Id: ulid.Make(), Operator: peer.StateUpdateOperator(operator), Operand: operandInt}, &reply)
	if err2 != nil {
		return "", err2
	}

	if reply.OK {
		return fmt.Sprintf("%s: %#v -> %#v", name, reply.Before, reply.After), nil
	} else {
		return "update failed", nil
	}
}

func ListDiff(name string) (string, error) {
	var reply peer.RequestDiffsReply
	err := send_rpc(name, "Worker.RequestDiffs", peer.RequestDiffsArgs{}, &reply)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%#v", reply.Diffs), nil
}

func Leader(name string) (string, error) {
	var reply peer.RequestLeaderReply
	err := send_rpc(name, "Worker.RequestLeader", peer.RequestLeaderReply{}, &reply)
	if err != nil {
		return "", err
	}
	return reply.Leader, nil
}
