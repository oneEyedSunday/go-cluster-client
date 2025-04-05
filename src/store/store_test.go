package store

import "testing"

func Test_StoreOpen(t *testing.T) {
	s := New("127.0.0.1:8088", "/tmp/raft", false)
	if s == nil {
		t.Fatalf("failed to create store")
	}
}
