// Package config loads cluster membership for a single node from
// environment variables, which is all docker-compose.yml needs to
// set per-container.
package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/Atharva9890/raft-kv-store/raft"
)

// NodeConfig is everything cmd/node/main.go needs to start one
// cluster member.
type NodeConfig struct {
	Raft       raft.Config
	ListenAddr string // e.g. ":9001" - this node's own gRPC listen address
}

// FromEnv reads:
//
//	NODE_ID      this node's id, must be a key in PEERS      (required)
//	PEERS        "id1=host1:port1,id2=host2:port2,..."       (required, must include NODE_ID)
//	LISTEN_ADDR  address this node listens on, default ":9001"
//
// Example for a 5-node docker-compose cluster, set on the "node3"
// container:
//
//	NODE_ID=node3
//	PEERS=node1=node1:9001,node2=node2:9001,node3=node3:9001,node4=node4:9001,node5=node5:9001
func FromEnv() (NodeConfig, error) {
	id := os.Getenv("NODE_ID")
	if id == "" {
		return NodeConfig{}, fmt.Errorf("NODE_ID is required")
	}

	peersRaw := os.Getenv("PEERS")
	if peersRaw == "" {
		return NodeConfig{}, fmt.Errorf("PEERS is required")
	}
	peers := make(map[raft.PeerID]string)
	for _, entry := range strings.Split(peersRaw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 {
			return NodeConfig{}, fmt.Errorf("malformed PEERS entry %q, want id=host:port", entry)
		}
		peers[raft.PeerID(parts[0])] = parts[1]
	}
	if _, ok := peers[raft.PeerID(id)]; !ok {
		return NodeConfig{}, fmt.Errorf("NODE_ID %q not found in PEERS", id)
	}

	listenAddr := os.Getenv("LISTEN_ADDR")
	if listenAddr == "" {
		listenAddr = ":9001"
	}

	return NodeConfig{
		Raft: raft.Config{
			Self:  raft.PeerID(id),
			Peers: peers,
		},
		ListenAddr: listenAddr,
	}, nil
}
