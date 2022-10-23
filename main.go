package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	httpd "github.com/oneeyedsunday/go-cluster-client/src/http"
	"github.com/oneeyedsunday/go-cluster-client/src/store"
)

type config struct {
	httpAddr, raftAddr, joinAddr, raftDir string
}

const (
	DefaultHTTPAddr = "127.0.0.1:11000"
	DefaultRaftAddr = "127.0.0.1:12000"
)

// Flag set
var fs *flag.FlagSet

func main() {
	fs = flag.NewFlagSet("hraftd", flag.ExitOnError)

	var cfg config

	cfg.httpAddr = *fs.String("httpAddr", DefaultHTTPAddr, "Set HTTP bind address")
	fs.StringVar(&cfg.raftAddr, "raftAddr", DefaultRaftAddr, "Set Raft bind address")
	fs.StringVar(&cfg.raftDir, "raftDir", "/tmp/raft", "Set storage path for Raft")
	flag.StringVar(&cfg.joinAddr, "joinDir", "", "Set join address, if any")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] <raft-data-path> \n", os.Args[0])
		flag.PrintDefaults()
	}

	err := fs.Parse(os.Args)
	if err != nil {
		log.Fatalf("failed to parse.")
	}

	// Ensure Raft storage exists.
	if cfg.raftDir == "" {
		fmt.Fprintf(os.Stderr, "No Raft storage directory specified\n")
		os.Exit(1)
	}
	os.MkdirAll(cfg.raftDir, 0700)

	s := store.New(cfg.raftAddr, cfg.raftDir)
	if err := s.Open(cfg.joinAddr != ""); err != nil {
		log.Fatalf("failed to open store: %s", err.Error())
	}

	// if theres a join address, make the join request
	if *&cfg.joinAddr != "" {
		if err := join(cfg.joinAddr, cfg.raftAddr); err != nil {
			log.Fatalf("failed to join node at %s: %s", cfg.joinAddr, err.Error())
		}
	}

	n := httpd.New(cfg.httpAddr, s)

	if err = n.Start(); err != nil {
		log.Fatalf("failed to start server %s", err.Error())
	}

	log.Println("hraft started successfully")

	select {}
}

func join(joinAddr, raftAddr string) error {
	b, err := json.Marshal(map[string]string{"addr": raftAddr})
	if err != nil {
		return err
	}

	resp, err := http.Post(fmt.Sprintf("http://%s/join", joinAddr), "application-type/json", bytes.NewReader((b)))
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	return nil
}
