package store

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftBoltDb "github.com/hashicorp/raft-boltdb"
)

const (
	retainSnapshotCount = 3
	raftTimeout         = 5 * time.Second
)

type command struct {
	Op    string `json:"op,omitempty"`
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

type RaftStore struct {
	rDir, rBind string
	mu          sync.Mutex
	m           map[string]string

	raft   *raft.Raft // The consensus mechanism
	logger *log.Logger
}

func New(raftBind, raftDir string) *RaftStore {
	return &RaftStore{
		rBind:  raftBind,
		rDir:   raftDir,
		m:      make(map[string]string),
		logger: log.New(os.Stdout, "[store] ", log.LstdFlags),
	}
}

func (s *RaftStore) Open(enableSingle bool) error {
	config := raft.DefaultConfig()

	// Setup Raft communication.
	addr, err := net.ResolveTCPAddr("tcp", s.rBind)
	if err != nil {
		return err
	}

	transport, err := raft.NewTCPTransport(s.rBind, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}

	config.LocalID = raft.ServerID(transport.LocalAddr())

	// Create peer storage.
	// peerStore := raft.NewJSONPeers("/tmp/raft/log.json", transport)

	// Create the log store and stable store.
	logStore, err := raftBoltDb.NewBoltStore(filepath.Join(s.rDir, "raft.db"))
	if err != nil {
		return fmt.Errorf("new bolt store: %s", err)
	}

	// Create the snapshot store.
	snapshots, err := raft.NewFileSnapshotStore(s.rDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	// Create raft log.
	ra, err := raft.NewRaft(config, s, logStore, logStore, snapshots, transport)

	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}

	s.raft = ra
	return nil
}

// Get only reads local results
func (s *RaftStore) Get(key string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.m[key], nil
}

func (s *RaftStore) Set(key, value string) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	c := &command{
		Op:    "set",
		Key:   key,
		Value: value,
	}

	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)
	// if err, ok := f.(error); ok {
	// 	return err
	// }

	return f.Error()
}

func (s *RaftStore) Delete(key string) error {
	return nil
}

func (s *RaftStore) Join(addr string) error {
	s.logger.Printf("received join request for remote node as %s", addr)

	f := s.raft.AddPeer(raft.ServerAddress(addr))
	if f.Error() != nil {
		return f.Error()
	}

	s.logger.Printf("node at %s joined successfully", addr)
	return nil
}

func (f *RaftStore) Apply(l *raft.Log) interface{} {
	var c command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	switch c.Op {
	case "set":
		return f.applySet(c.Key, c.Value)
	case "delete":
		return f.applyDelete(c.Key)
	default:
		panic(fmt.Sprintf("unrecognized command op: %s", c.Op))
	}
}

func (f *RaftStore) Snapshot() (raft.FSMSnapshot, error) {
	return nil, nil
}

func (f *RaftStore) Restore(rc io.ReadCloser) error {
	return nil
}

func (f *RaftStore) applySet(key, value string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.m[key] = value
	return nil
}

func (f *RaftStore) applyDelete(key string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.m, key)
	return nil
}
