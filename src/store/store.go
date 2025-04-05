package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	"github.com/hashicorp/raft"

	raftBoltDb "github.com/hashicorp/raft-boltdb"
)

const (
	retainSnapshotCount = 3
	raftTimeout         = 5 * time.Second
)

var errNotLeader = errors.New("not leader")

type command struct {
	Op    string `json:"op,omitempty"`
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

// Node represents a node in the cluster.
type Node struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

// StoreStatus represents a status of the Store
type StoreStatus struct {
	Me        Node   `json:"me"`
	Leader    Node   `json:"leader"`
	Followers []Node `json:"followers"`
}

type RaftStore struct {
	rDir, rBind string
	inmem       bool
	mu          sync.Mutex

	m map[string]string

	raft   *raft.Raft // The consensus mechanism
	logger *log.Logger
}

func New(raftBind, raftDir string, inmem bool) *RaftStore {
	return &RaftStore{
		rBind:  raftBind,
		rDir:   raftDir,
		m:      make(map[string]string),
		inmem:  inmem,
		logger: log.New(os.Stdout, "[store] ", log.LstdFlags),
	}
}

func (s *RaftStore) Open(localID string, enableSingle bool) error {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(localID)

	config.HeartbeatTimeout = time.Duration(2 * time.Second)
	config.ElectionTimeout = time.Duration(2 * time.Second)

	// Setup Raft communication.
	addr, err := net.ResolveTCPAddr("tcp", s.rBind)
	if err != nil {
		return err
	}

	// fmt.Println(addr)

	transport, err := raft.NewTCPTransport(s.rBind, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}

	// fmt.Println(transport)

	config.LocalID = raft.ServerID(transport.LocalAddr())

	// fmt.Printf("config.LocalID is %s while localId is %s\n", config.LocalID, localID)

	// Create peer storage.
	// peerStore := raft.NewJSONPeers("/tmp/raft/log.json", transport)

	// Create the log store and stable store.

	fmt.Println("creating logStore")
	var logStore raft.LogStore
	var stableStore raft.StableStore
	if s.inmem {
		logStore = raft.NewInmemStore()
		stableStore = raft.NewInmemStore()
	} else {
		boltDB, err := raftBoltDb.New(raftBoltDb.Options{
			Path: filepath.Join(s.rDir, "raft.db"),
			BoltOptions: &bolt.Options{
				Timeout: time.Duration(time.Second * 10),
				// ReadOnly: true,
			},
		})

		if err != nil {
			fmt.Println("fail to create log")
			return fmt.Errorf("new bolt store: %s", err)
		}

		logStore = boltDB
		stableStore = boltDB
	}

	fmt.Println("after logStore")

	// Create the snapshot store.
	snapshots, err := raft.NewFileSnapshotStore(s.rDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	fmt.Println("after snapshot")

	// Create raft subsystem.
	ra, err := raft.NewRaft(config, s, logStore, stableStore, snapshots, transport)

	if err != nil {
		fmt.Printf("err with newRaft")
		return fmt.Errorf("new raft: %s", err)
	}

	fmt.Println("before enableSIngle")

	if enableSingle {
		s.logger.Println("ENabling single mode")
		if err := ra.BootstrapCluster(raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: raft.ServerAddress(transport.LocalAddr()),
				},
			},
		}).Error(); err != nil {
			s.logger.Fatal(err)
		}
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
		return errNotLeader
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
	if s.raft.State() != raft.Leader {
		return errNotLeader
	}

	c := &command{
		Op:  "delete",
		Key: key,
	}

	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)
	return f.Error()
}

func (s *RaftStore) Join(addr string) error {
	s.logger.Printf("received join request for remote node as %s", addr)

	f := s.raft.AddVoter(raft.ServerID(addr), raft.ServerAddress(addr), 0, 0)
	if f.Error() != nil {
		return f.Error()
	}

	s.logger.Printf("node at %s joined successfully", addr)
	return nil
}

func (f *RaftStore) Status() (StoreStatus, error) {
	leaderServerAddr, leaderId := f.raft.LeaderWithID()
	leader := Node{
		ID:      string(leaderId),
		Address: string(leaderServerAddr),
	}

	me := Node{
		Address: f.rBind,
	}

	servers := f.raft.GetConfiguration().Configuration().Servers
	followers := make([]Node, 0, len(servers))

	for _, server := range servers {
		if server.ID != leaderId {
			followers = append(followers, Node{
				ID:      string(server.ID),
				Address: string(server.Address),
			})
		}

		if string(server.Address) == f.rBind {
			me.ID = string(server.ID) // no need to create new object
		}
	}

	return StoreStatus{Leader: leader, Me: me, Followers: followers}, nil
}

// Apply applies a Raft log entry to the k-v store.
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

type fsmSnapshot struct {
	store map[string]string
}

func (f *RaftStore) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Clone the data
	o := make(map[string]string)
	for k, v := range f.m {
		o[k] = v
	}

	return &fsmSnapshot{store: o}, nil
}

func (f *RaftStore) Restore(rc io.ReadCloser) error {
	o := make(map[string]string)

	if err := json.NewDecoder(rc).Decode(&o); err != nil {
		return err
	}

	// err, why are not locking??
	// https://github.com/otoolep/hraftd/blob/master/store/store.go#L295
	f.m = o
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

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		b, err := json.Marshal(f.store)
		if err != nil {
			return err
		}

		if _, err := sink.Write(b); err != nil {
			return err
		}
		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
	}

	return err
}

func (f *fsmSnapshot) Release() {
	log.Println("releasing snapshot")
}
