package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"quorum/internal/server"
	"quorum/internal/statemachine"
	pb "quorum/proto"

	"google.golang.org/grpc"
)

func main() {
	var cfg server.Config
	flag.Uint64Var(&cfg.NodeID, "node-id", 1, "stable node ID")
	flag.StringVar(&cfg.HTTPAddr, "http-addr", ":8080", "health and readiness listen address")
	flag.StringVar(&cfg.ClientAddr, "client-addr", ":9090", "client gRPC listen address")
	flag.StringVar(&cfg.PeerAddr, "peer-addr", ":9000", "peer listen address")
	flag.StringVar(&cfg.DataDir, "data-dir", "data", "durable data directory")
	flag.Parse()

	raftNode, err := server.OpenPersistentRaft(cfg.DataDir, cfg.NodeID, []uint64{cfg.NodeID})
	if err != nil {
		log.Fatalf("open raft runtime: %v", err)
	}
	defer func() {
		if err := raftNode.Close(); err != nil {
			log.Printf("close raft runtime: %v", err)
		}
	}()
	raftNode.Node().SetElectionTimeout(1)
	if _, err := raftNode.Tick(); err != nil {
		log.Fatalf("bootstrap single-node leader: %v", err)
	}

	machine := statemachine.NewKV()
	if err := server.ReplayCommittedLog(cfg.DataDir, machine); err != nil {
		log.Fatalf("replay committed log: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterQuorumServer(grpcServer, server.NewKVService(raftNode, machine))
	clientListener, err := net.Listen("tcp", cfg.ClientAddr)
	if err != nil {
		log.Fatalf("listen client grpc: %v", err)
	}

	health := server.NewHealthServer()
	health.SetReady(true)
	httpServer := &http.Server{Addr: cfg.HTTPAddr, Handler: health.Handler()}

	go func() {
		log.Printf("quorumd node=%d http=%s client=%s peer=%s data=%s", cfg.NodeID, cfg.HTTPAddr, cfg.ClientAddr, cfg.PeerAddr, cfg.DataDir)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("health server failed: %v", err)
		}
	}()
	go func() {
		if err := grpcServer.Serve(clientListener); err != nil {
			log.Fatalf("client grpc server failed: %v", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	health.SetReady(false)
	grpcServer.GracefulStop()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(ctx); err != nil {
		log.Fatalf("health shutdown failed: %v", err)
	}
}
