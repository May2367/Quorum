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
	"quorum/internal/transport"
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
	flag.StringVar(&cfg.PeerMap, "peers", "", "static peer map as id=host:port,id=host:port")
	flag.Parse()

	peers, err := server.ParsePeerMap(cfg.PeerMap, cfg.NodeID, cfg.PeerAddr)
	if err != nil {
		log.Fatalf("parse peers: %v", err)
	}

	var sender server.ReadyMessageSender
	if len(peers.IDs) > 1 {
		sender = transport.NewPeerClient(peers.Addrs, 5*time.Second)
	}
	asyncOpts := server.AsyncRaftOptions{
		Sender:      sender,
		SendTimeout: 5 * time.Second,
	}
	if len(peers.IDs) == 1 {
		asyncOpts.ElectionTimeout = 1
	}
	machine := statemachine.NewKV()
	if err := server.ReplayCommittedLog(cfg.DataDir, machine); err != nil {
		log.Fatalf("replay committed log: %v", err)
	}
	kvService := server.NewKVService(nil, machine)
	asyncOpts.Apply = kvService.ApplyCommitted
	raftNode, err := server.OpenAsyncRaft(cfg.DataDir, cfg.NodeID, peers.IDs, asyncOpts)
	if err != nil {
		log.Fatalf("open raft runtime: %v", err)
	}
	kvService.SetRaftRuntime(raftNode)
	defer func() {
		if err := raftNode.Close(); err != nil {
			log.Printf("close raft runtime: %v", err)
		}
	}()
	if len(peers.IDs) == 1 {
		if _, err := raftNode.Tick(); err != nil {
			log.Fatalf("bootstrap single-node leader: %v", err)
		}
	}

	clientGRPCServer := grpc.NewServer()
	pb.RegisterQuorumServer(clientGRPCServer, kvService)
	clientListener, err := net.Listen("tcp", cfg.ClientAddr)
	if err != nil {
		log.Fatalf("listen client grpc: %v", err)
	}

	peerGRPCServer := grpc.NewServer()
	pb.RegisterRaftPeerServer(peerGRPCServer, transport.NewPeerServer(raftNode))
	peerListener, err := net.Listen("tcp", cfg.PeerAddr)
	if err != nil {
		log.Fatalf("listen peer grpc: %v", err)
	}

	health := server.NewHealthServer()
	health.SetReady(true)
	httpServer := &http.Server{Addr: cfg.HTTPAddr, Handler: health.Handler()}
	stopTicks := make(chan struct{})

	go func() {
		log.Printf("quorumd node=%d http=%s client=%s peer=%s peers=%v data=%s", cfg.NodeID, cfg.HTTPAddr, cfg.ClientAddr, cfg.PeerAddr, peers.IDs, cfg.DataDir)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("health server failed: %v", err)
		}
	}()
	go func() {
		if err := clientGRPCServer.Serve(clientListener); err != nil {
			log.Fatalf("client grpc server failed: %v", err)
		}
	}()
	go func() {
		if err := peerGRPCServer.Serve(peerListener); err != nil {
			log.Fatalf("peer grpc server failed: %v", err)
		}
	}()
	go func() {
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if _, err := raftNode.Tick(); err != nil {
					log.Printf("raft tick failed: %v", err)
				}
			case <-stopTicks:
				return
			}
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	close(stopTicks)
	health.SetReady(false)
	clientGRPCServer.GracefulStop()
	peerGRPCServer.GracefulStop()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(ctx); err != nil {
		log.Fatalf("health shutdown failed: %v", err)
	}
}
