package httpd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/oneeyedsunday/go-cluster-client/src/store"
)

type testServer struct {
	*Service
}

func (t *testServer) URL() string {
	port := strings.TrimLeft(t.Addr().String(), "[::]:")
	return fmt.Sprintf("http://127.0.0.1:%s", port)
}

type testStore struct {
	m map[string]string
}

func newTestStore() *testStore {
	return &testStore{
		m: make(map[string]string),
	}
}

func Test_NewServer(t *testing.T) {
	// cant use this; nil map
	// store := new(testStore)
	store := newTestStore()
	s := &testServer{New(":0", store)}

	if err := s.Start(); err != nil {
		t.Fatalf("failed to start HTTP service: %s", err)
	}

	b := doGet(t, s.URL(), "k1")
	if string(b) != `{"k1":""}` {
		t.Fatalf("wrong value received for key k1: %s", string(b))
	}

	doPost(t, s.URL(), "k1", "v1")

	b = doGet(t, s.URL(), "k1")
	if string(b) != `{"k1":"v1"}` {
		t.Fatalf("wrong value received for key k1: %s", string(b))
	}

	store.m["k2"] = "v2"
	b = doGet(t, s.URL(), "k2")
	if string(b) != `{"k2":"v2"}` {
		t.Fatalf("wrong value received for key k2: %s", string(b))
	}

	doDelete(t, s.URL(), "k2")
	b = doGet(t, s.URL(), "k2")
	if string(b) != `{"k2":""}` {
		t.Fatalf("wrong value received for key k2: %s", string(b))
	}
}

func (t *testStore) Get(key string) (string, error) {
	return t.m[key], nil
}

func (t *testStore) Set(key string, value string) error {
	t.m[key] = value
	return nil
}

func (t *testStore) Delete(key string) error {
	delete(t.m, key)
	return nil
}

func (t *testStore) Join(nodeID, addr string) error {
	return nil
}

func (t *testStore) Status() (store.StoreStatus, error) {
	return store.StoreStatus{}, nil
}

func doGet(t *testing.T, url, key string) string {
	resp, err := http.Get(fmt.Sprintf("%s/key/%s", url, key))
	if err != nil {
		t.Fatalf("failed to GET key: %s", err)
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response: %s", err)
	}

	return string(body)
}

func doPost(t *testing.T, url, key, value string) {
	b, err := json.Marshal(map[string]string{key: value})
	if err != nil {
		t.Fatalf("failed to encode key and value for POST: %s", err)
	}

	resp, err := http.Post(fmt.Sprintf("%s/key", url), "application-type/json", bytes.NewReader(b))
	if err != nil {
		t.Fatalf("failed to POST key: %s", err)
	}

	defer resp.Body.Close()

	if err != nil {
		t.Fatalf("POST request failed: %s", err)
	}
}

func doDelete(t *testing.T, u, key string) {
	ru, err := url.Parse(fmt.Sprintf("%s/key/%s", u, key))
	if err != nil {
		t.Fatalf("failed to parse URL for delete: %s", err)
	}

	req := &http.Request{
		Method: "DLEETE",
		URL:    ru,
	}

	client := http.Client{}

	resp, err := client.Do(req)

	if err != nil {
		t.Fatalf("failed to GET key: %s", err)
	}
	defer resp.Body.Close()

	// why not just
	// http.Delete
	// because it doesnt exist
}
