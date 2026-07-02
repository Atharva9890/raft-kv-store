// this is one cluster member: a raft.Node plus the gRPC server
// exposing both the client-facing KV service and the internal Raft
// peer service on the same listener.
package main

import (
	"log"
	"net"
	"time"

	"google.golang.org/grpc"

	"github.com/Atharva9890/raft-kv-store/config"
	"github.com/Atharva9890/raft-kv-store/proto/kvpb"
	"github.com/Atharva9890/raft-kv-store/raft"
	"github.com/Atharva9890/raft-kv-store/server"
)

func main() {
	cfg, err := config.FromEnv()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	addrs := make(map[raft.PeerID]string, len(cfg.Raft.Peers))
	for id, addr := range cfg.Raft.Peers {
		addrs[id] = addr
	}
	transport := server.NewGRPCTransport(addrs)

	persister, err := raft.NewFilePersister(cfg.DataDir)
	if err != nil {
		log.Fatalf("persister: %v", err)
	}

	applyCh := make(chan raft.ApplyMsg, 256)
	node := raft.NewNode(cfg.Raft, transport, persister, applyCh)

	sm := server.NewKVStore()
	kvServer := server.NewServer(node, sm, cfg.PublicAddrs, applyCh)
	raftService := server.NewRaftService(node)

	node.Run()
	go reportStatus(node, cfg.Raft.Self)

	lis, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		log.Fatalf("listen on %s: %v", cfg.ListenAddr, err)
	}

	grpcServer := grpc.NewServer()
	kvpb.RegisterKVServer(grpcServer, kvServer)
	kvpb.RegisterRaftServer(grpcServer, raftService)

	log.Printf("node %s listening on %s (peers: %v, data dir: %s)", cfg.Raft.Self, cfg.ListenAddr, cfg.Raft.OtherPeers(), cfg.DataDir)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

// prints role/term whenever either one changes, so `docker compose
// logs -f` shows the election happening live without me having to
// attach a debugger to watch it.
func reportStatus(node *raft.Node, self raft.PeerID) {
	var lastTerm uint64
	var lastLeader bool
	first := true
	for range time.Tick(500 * time.Millisecond) {
		term, isLeader := node.State()
		if first || term != lastTerm || isLeader != lastLeader {
			role := "follower/candidate"
			if isLeader {
				role = "LEADER"
			}
			log.Printf("[%s] term=%d role=%s", self, term, role)
		}
		lastTerm, lastLeader, first = term, isLeader, false
	}
}
