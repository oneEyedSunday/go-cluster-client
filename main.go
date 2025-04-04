package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	httpd "github.com/oneeyedsunday/go-cluster-client/src/http"
	"github.com/oneeyedsunday/go-cluster-client/src/store"
)

type config struct {
	httpAddr, raftAddr, joinAddr, raftDir, nodeID string
	inmem                                         bool
}

const (
	DefaultHTTPAddr = "127.0.0.1:11000"
	DefaultRaftAddr = "127.0.0.1:12000"
)

var cfg config

func initConfig() {
	flag.StringVar(&cfg.httpAddr, "httpAddr", DefaultHTTPAddr, "Set HTTP bind address")
	flag.StringVar(&cfg.raftAddr, "raftAddr", DefaultRaftAddr, "Set Raft bind address")
	flag.StringVar(&cfg.nodeID, "nodeId", "", "Node Id")
	flag.StringVar(&cfg.raftDir, "raftDir", "/tmp/raft", "Set storage path for Raft")
	flag.StringVar(&cfg.joinAddr, "joinAddr", "", "Set join address, if any")
	flag.BoolVar(&cfg.inmem, "inmem", false, "Use in-memory storage for Raft")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] <raft-data-path> \n", os.Args[0])
		flag.PrintDefaults()
	}

	flag.Parse()
}

func main() {

	initConfig()

	fmt.Printf("Running with config: %v\n", cfg)

	// Ensure Raft storage exists.
	if cfg.raftDir == "" {
		fmt.Fprintf(os.Stderr, "No Raft storage directory specified\n")
		os.Exit(1)
	}

	if cfg.nodeID == "" {
		fmt.Fprintf(os.Stderr, "No nodeId speciifed\n")
		os.Exit(1)
	}

	os.MkdirAll(cfg.raftDir, 0700)

	s := store.New(cfg.raftAddr, cfg.raftDir, cfg.inmem)
	if err := s.Open(cfg.nodeID, cfg.joinAddr == ""); err != nil {
		log.Fatalf("failed to open store: %s", err.Error())
	}

	// if theres a join address, make the join request
	if cfg.joinAddr != "" {
		if err := join(cfg); err != nil {
			log.Fatalf("failed to join node at %s: %s", cfg.joinAddr, err.Error())
		}
	}

	// TODO sanely remove servers on exit

	n := httpd.New(cfg.httpAddr, s)

	if err := n.Start(); err != nil {
		log.Fatalf("failed to start server %s", err.Error())
	}

	log.Printf("hraft started successfully, listening on http://%s\n", cfg.httpAddr)

	acabar := make(chan os.Signal, 1)
	signal.Notify(acabar, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	<-acabar

	log.Println("hraft exiting")
}

func join(cfg config) error {
	b, err := json.Marshal(map[string]string{"addr": cfg.raftAddr, "id": cfg.nodeID})
	if err != nil {
		return err
	}

	log.Printf("attempting to join node %s from %s\n", cfg.joinAddr, cfg.raftAddr)

	resp, err := http.Post(fmt.Sprintf("http://%s/join", cfg.joinAddr), "application-type/json", bytes.NewReader((b)))
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	return nil
}
