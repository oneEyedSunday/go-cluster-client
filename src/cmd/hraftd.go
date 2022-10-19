package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	httpd "github.com/oneeyedsunday/go-cluster-client/src/http"
	"github.com/oneeyedsunday/go-cluster-client/src/store"
)

type config struct {
	httpAddr, raftAddr, joinAddr, raftDir string
}

const (
	DefaultHTTPAddr = ":11000"
	DefaultRaftAddr = ":11001"
)

// Flag set
var fs *flag.FlagSet

func main() {
	fs = flag.NewFlagSet("hraftd", flag.ExitOnError)

	var cfg config

	cfg.httpAddr = *fs.String("httpAddr", DefaultHTTPAddr, "Set HTTP bind address")
	fs.StringVar(&cfg.raftAddr, "raftAddr", DefaultRaftAddr, "Set Raft bind address")
	fs.StringVar(&cfg.raftDir, "raftDir", "/tmp/raft", "Set storage path for Raft")

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
	if err := s.Open(); err != nil {
		log.Fatalf("failed to open store: %s", err.Error())
	}

	n := httpd.New(cfg.httpAddr, s)

	fmt.Print(n)

	log.Println("hraft started successfully")

	select {}
}
