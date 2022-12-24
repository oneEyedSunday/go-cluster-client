package httpd

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strings"
)

type Store interface {
	Get(key string) (string, error)
	Set(key string, value string) error
	Delete(key string) error
	// Join joins the node reachable at addr to the cluster
	Join(addr string) error
}

type Service struct {
	addr string
	ln   net.Listener
	// every service will have its store???
	store Store
}

func New(addr string, store Store) *Service {
	return &Service{
		addr:  addr,
		store: store,
	}
}

func (s *Service) Start() error {
	server := http.Server{
		Handler: s,
	}

	ln, err := net.Listen("tcp", s.addr)

	if err != nil {
		return err
	}

	s.ln = ln

	http.Handle("/", s)

	go func() {
		err := server.Serve(s.ln)
		if err != nil {
			log.Fatalf("HTTP serve: %s", err)
		}
	}()

	return nil
}

func (s *Service) Close() error {
	return s.ln.Close()
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/key") {
		s.handleKeyOps(w, r)
	} else if r.URL.Path == "/join" {
		s.handleJoin(w, r)
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

func (s *Service) handleJoin(w http.ResponseWriter, r *http.Request) {
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	m := map[string]string{}
	if err := json.Unmarshal(b, &m); err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// empty map / request
	if len(m) != 1 {
		log.Println(err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	remoteAddr, ok := m["addr"]
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if err := s.store.Join(remoteAddr); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func (s *Service) handleKeyOps(w http.ResponseWriter, r *http.Request) {
	// refactor this into something more usable
	getKeyFromRequest := func() string {
		parts := strings.Split(r.URL.Path, "/")

		if len(parts) != 3 {
			// base path is "/key"
			return string("")
		}

		// what happens to /key/foo/bar/baz???
		return string(parts[2])
	}

	switch r.Method {
	case "GET":
		k := getKeyFromRequest()
		if k == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		v, err := s.store.Get(k)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		b, err := json.Marshal(map[string]string{k: v})

		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		io.WriteString(w, string(b))
	case "POST":
		// Read the value from the POST body.
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		m := map[string]string{}

		if err := json.Unmarshal(b, &m); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		for k, v := range m {
			if err := s.store.Set(k, v); err != nil {
				log.Println(err.Error())
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}
	case "DELETE":
		k := getKeyFromRequest()
		if k == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if err := s.store.Delete(k); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
	return
}

func (s *Service) Addr() net.Addr {
	return s.ln.Addr()
}
