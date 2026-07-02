// I load cluster membership for a single node straight from env vars -
// that's literally all docker-compose.yml needs to set per container,
// no config file mounting or anything fancier.
package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/Atharva9890/raft-kv-store/raft"
)

// everything cmd/node/main.go needs to bring one cluster member up.
type NodeConfig struct {
	Raft       raft.Config
	ListenAddr string // this node's own gRPC listen address, e.g. ":9001"
	DataDir    string // where FilePersister writes state.json / snapshot.json

	// how an outside client should dial each node, as opposed to
	// Raft.Peers which is how nodes dial each other. these are
	// usually different: inside docker-compose, nodes reach each
	// other over the compose network by container name ("node3:9001"),
	// but a client running on my laptop needs the host-mapped port
	// ("localhost:9103") instead. I found this out the hard way when
	// kvctl kept failing to follow a NotLeader redirect - the hint
	// coming back was a raft.PeerID like "node3", not anything
	// dialable from outside the container network.
	PublicAddrs map[raft.PeerID]string
}

// reads:
//
//	NODE_ID       this node's id, must be a key in PEERS      (required)
//	PEERS         "id1=host1:port1,id2=host2:port2,..."       (required, must include NODE_ID)
//	PUBLIC_ADDRS  same format as PEERS, but addresses a client outside
//	              the cluster network can actually dial. defaults to
//	              PEERS if unset, which is fine as long as clients and
//	              nodes are on the same network.
//	LISTEN_ADDR   address this node listens on, default ":9001"
//	DATA_DIR      where to persist state, default "data/<NODE_ID>"
//
// what I actually set on, say, the node3 container in docker-compose:
//
//	NODE_ID=node3
//	PEERS=node1=node1:9001,node2=node2:9001,node3=node3:9001,node4=node4:9001,node5=node5:9001
//	PUBLIC_ADDRS=node1=localhost:9101,node2=localhost:9102,node3=localhost:9103,node4=localhost:9104,node5=localhost:9105
func FromEnv() (NodeConfig, error) {
	id := os.Getenv("NODE_ID")
	if id == "" {
		return NodeConfig{}, fmt.Errorf("NODE_ID is required")
	}

	peers, err := parsePeerMap(os.Getenv("PEERS"))
	if err != nil {
		return NodeConfig{}, fmt.Errorf("PEERS: %w", err)
	}
	if len(peers) == 0 {
		return NodeConfig{}, fmt.Errorf("PEERS is required")
	}
	if _, ok := peers[raft.PeerID(id)]; !ok {
		return NodeConfig{}, fmt.Errorf("NODE_ID %q not found in PEERS", id)
	}

	publicAddrs := peers
	if raw := os.Getenv("PUBLIC_ADDRS"); raw != "" {
		publicAddrs, err = parsePeerMap(raw)
		if err != nil {
			return NodeConfig{}, fmt.Errorf("PUBLIC_ADDRS: %w", err)
		}
	}

	listenAddr := os.Getenv("LISTEN_ADDR")
	if listenAddr == "" {
		listenAddr = ":9001"
	}

	dataDir := os.Getenv("DATA_DIR")
	if dataDir == "" {
		dataDir = fmt.Sprintf("data/%s", id)
	}

	return NodeConfig{
		Raft: raft.Config{
			Self:  raft.PeerID(id),
			Peers: peers,
		},
		ListenAddr:  listenAddr,
		DataDir:     dataDir,
		PublicAddrs: publicAddrs,
	}, nil
}

func parsePeerMap(raw string) (map[raft.PeerID]string, error) {
	m := make(map[raft.PeerID]string)
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("malformed entry %q, want id=host:port", entry)
		}
		m[raft.PeerID(parts[0])] = parts[1]
	}
	return m, nil
}
