package store

import (
	"os"
	"testing"
	"time"
)

func Test_StoreOpen(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "store_test")
	defer os.RemoveAll(tmpDir)
	s := New("127.0.0.1:8088", tmpDir, false)
	if s == nil {
		t.Fatalf("failed to create store")
	}

	if err := s.Open("init_0", false); err != nil {
		t.Fatalf("failed to open store: %s", err)
	}
}

func assertOps(t *testing.T, s *RaftStore) {
	if err := s.Open("init_0", true); err != nil {
		t.Fatalf("failed to open store: %s", err)
	}

	<-time.Tick(time.Second * 5)

	if err := s.Set("alice", "bob"); err != nil {
		t.Fatalf("failed to set key: %s", err)
	}

	<-time.Tick(500 * time.Millisecond)
	value, err := s.Get("alice")
	if err != nil {
		t.Fatalf("failed to get key: %s", err)
	}
	if value != "bob" {
		t.Fatalf("key has wrong value: %s", value)
	}

	if err := s.Delete("alice"); err != nil {
		t.Fatalf("failed to delete key: %s", err)
	}

	<-time.Tick(500 * time.Millisecond)
	value, err = s.Get("alice")
	if err != nil {
		t.Fatalf("failed to get key: %s", err)
	}
	if value != "" {
		t.Fatalf("key has wrong value: %s", value)
	}
}

func Test_StoreInMemory(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "store_test")
	defer os.RemoveAll(tmpDir)
	s := New("127.0.0.1:0", tmpDir, true)
	if s == nil {
		t.Fatalf("failed to create store")
	}

	assertOps(t, s)
}

func Test_StoreSingleNode(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "store_test")
	defer os.RemoveAll(tmpDir)
	s := New("127.0.0.1:0", tmpDir, false)

	if s == nil {
		t.Fatalf("failed to create store")
	}

	assertOps(t, s)
}

func Test_StoreStatus(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "store_test")
	defer os.RemoveAll(tmpDir)
	s := New("127.0.0.1:0", tmpDir, false)

	if s == nil {
		t.Fatalf("failed to create store")
	}

	if err := s.Open("node0", true); err != nil {
		t.Fatalf("failed to open store: %s", err)
	}

	<-time.Tick(time.Second * 5)

	status, err := s.Status()
	if err != nil {
		t.Errorf("failed to get status from store: %s", err)
	}

	if status.Me.ID != "node0" {
		t.Errorf("unexpected local id: %s", status.Me.ID)
	}

	if status.Me.Address != s.rBind {
		t.Errorf("unexpected local address: %s", status.Me.Address)
	}

	for _, follower := range status.Followers {
		if (follower.ID == status.Leader.ID) || (follower.Address == status.Leader.Address) {
			t.Fatalf("invalid state: node cannot be leader and follower")
		}
	}
}
