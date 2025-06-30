package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
)

const shardCount = 4

// KeyRange defines a shard's key range
type KeyRange struct {
	ID    int
	Start byte
	End   byte
}

func (kr KeyRange) Contains(key string) bool {
	if len(key) == 0 {
		return false
	}
	b := key[0]
	return b >= kr.Start && b <= kr.End
}

func calculateKeyRanges(n int) []KeyRange {
	total := 94
	base := total / n
	extra := total % n
	start := byte(33)
	ranges := make([]KeyRange, 0, n)
	for i := 0; i < n; i++ {
		size := base
		if i < extra {
			size++
		}
		end := start + byte(size) - 1
		ranges = append(ranges, KeyRange{ID: i, Start: start, End: end})
		start = end + 1
	}
	return ranges
}

type Command struct {
	Key    string
	Args   []string
	RespCh chan string
}

type Shard struct {
	ID        int
	Range     KeyRange
	KeyRanges []KeyRange
	Store     map[string][]byte
	CmdCh     chan Command
	Peers     []chan Command
	Mutex     sync.RWMutex
}

func (s *Shard) run() {
	for cmd := range s.CmdCh {
		if !s.Range.Contains(cmd.Key) {
			target := s.findTargetShard(cmd.Key)
			if target >= 0 && target < len(s.Peers) {
				s.Peers[target] <- cmd
			} else {
				cmd.RespCh <- "ERR: invalid shard forward"
			}
			continue
		}
		s.process(cmd)
	}
}

func (s *Shard) process(cmd Command) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	var response string
	args := cmd.Args
	fmt.Println("Processing command on shard by goroutine:", s.ID, "Args:", args)
	switch strings.ToLower(args[0]) {
	case "set":
		if len(args) == 3 {
			s.Store[args[1]] = []byte(args[2])
			response = "OK"
		} else {
			response = "ERR: invalid SET"
		}
	case "get":
		if val, ok := s.Store[args[1]]; ok {
			response = string(val)
		} else {
			response = "(nil)"
		}
	case "del":
		if _, ok := s.Store[args[1]]; ok {
			delete(s.Store, args[1])
			response = "OK"
		} else {
			response = "(nil)"
		}
	default:
		response = "ERR: unknown command"
	}
	cmd.RespCh <- response
}

func (s *Shard) findTargetShard(key string) int {
	for _, r := range s.KeyRanges {
		if r.Contains(key) {
			return r.ID
		}
	}
	return -1
}

func main() {
	listener, err := net.Listen("tcp", ":6379")
	if err != nil {
		panic(err)
	}
	fmt.Println("Listening on :6379")

	keyRanges := calculateKeyRanges(shardCount)
	cmdChans := make([]chan Command, shardCount)
	shards := make([]*Shard, shardCount)

	for i := 0; i < shardCount; i++ {
		cmdChans[i] = make(chan Command, 128)
	}

	for i := 0; i < shardCount; i++ {
		shards[i] = &Shard{
			ID:        i,
			Range:     keyRanges[i],
			KeyRanges: keyRanges,
			Store:     make(map[string][]byte),
			CmdCh:     cmdChans[i],
			Peers:     cmdChans,
		}
		go shards[i].run()
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("accept error:", err)
			continue
		}
		go handleConnection(conn, cmdChans, keyRanges)
	}
}

func handleConnection(conn net.Conn, cmdChans []chan Command, ranges []KeyRange) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		args := strings.Fields(line)
		if len(args) < 2 {
			conn.Write([]byte("ERR: invalid command\n"))
			continue
		}
		key := args[1]
		respCh := make(chan string)
		cmd := Command{Key: key, Args: args, RespCh: respCh}
		shardID := findShardID(key, ranges)
		cmdChans[shardID] <- cmd
		response := <-respCh
		conn.Write([]byte(response + "\n"))
	}
}

func findShardID(key string, ranges []KeyRange) int {
	for _, r := range ranges {
		if r.Contains(key) {
			return r.ID
		}
	}
	return 0
}
