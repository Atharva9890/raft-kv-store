// I wrote this after someone pointed out I had a throughput number on
// my resume with nothing backing it up. This spins up a real 5-node
// cluster in-process - real gRPC, real FilePersister writing JSON to
// disk on every entry, no batching, one RPC per op - and hammers the
// leader directly with persistent connections to see what it actually
// does. Run it yourself before trusting any number here; it's going
// to depend a lot on your disk and CPU.
package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Atharva9890/raft-kv-store/proto/kvpb"
	"github.com/Atharva9890/raft-kv-store/raft"
	"github.com/Atharva9890/raft-kv-store/server"
)

const (
	numNodes    = 5
	concurrency = 50
	duration    = 5 * time.Second
	basePort    = 19101
)

func main() {
	dir, err := os.MkdirTemp("", "raft-bench-*")
	must(err)
	defer os.RemoveAll(dir)

	addrs := make(map[raft.PeerID]string, numNodes)
	for i := 0; i < numNodes; i++ {
		id := raft.PeerID(fmt.Sprintf("node%d", i+1))
		addrs[id] = fmt.Sprintf("localhost:%d", basePort+i)
	}

	nodes := make(map[raft.PeerID]*raft.Node, numNodes)
	for id, addr := range addrs {
		persister, err := raft.NewFilePersister(fmt.Sprintf("%s/%s", dir, id))
		must(err)
		transport := server.NewGRPCTransport(addrs)
		applyCh := make(chan raft.ApplyMsg, 1024)
		node := raft.NewNode(raft.Config{Self: id, Peers: addrs}, transport, persister, applyCh)
		nodes[id] = node

		sm := server.NewKVStore()
		kvServer := server.NewServer(node, sm, addrs, applyCh)
		raftService := server.NewRaftService(node)

		lis, err := net.Listen("tcp", addr)
		must(err)
		grpcServer := grpc.NewServer()
		kvpb.RegisterKVServer(grpcServer, kvServer)
		kvpb.RegisterRaftServer(grpcServer, raftService)
		go grpcServer.Serve(lis)

		node.Run()
	}

	fmt.Println("waiting for a leader...")
	leaderID := waitForLeader(nodes, 5*time.Second)
	leaderAddr := addrs[leaderID]
	fmt.Printf("leader is %s at %s - letting the post-election no-op settle\n\n", leaderID, leaderAddr)
	time.Sleep(300 * time.Millisecond)

	fmt.Printf("--- PUT: %d workers, persistent connections, %s ---\n", concurrency, duration)
	runBenchmark(leaderAddr, concurrency, duration, func(kc kvpb.KVClient, i int) error {
		resp, err := kc.Put(context.Background(), &kvpb.PutRequest{Key: fmt.Sprintf("k%d", i), Value: "some-benchmark-value"})
		if err != nil {
			return err
		}
		if resp.NotLeader {
			return fmt.Errorf("leader changed mid-benchmark")
		}
		return nil
	})

	fmt.Printf("--- GET: %d workers, persistent connections, %s ---\n", concurrency, duration)
	runBenchmark(leaderAddr, concurrency, duration, func(kc kvpb.KVClient, i int) error {
		resp, err := kc.Get(context.Background(), &kvpb.GetRequest{Key: fmt.Sprintf("k%d", i%1000)})
		if err != nil {
			return err
		}
		if resp.NotLeader {
			return fmt.Errorf("leader changed mid-benchmark")
		}
		return nil
	})
}

func waitForLeader(nodes map[raft.PeerID]*raft.Node, timeout time.Duration) raft.PeerID {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for id, n := range nodes {
			if _, isLeader := n.State(); isLeader {
				return id
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	panic("no leader elected within timeout")
}

func runBenchmark(addr string, concurrency int, duration time.Duration, op func(kvpb.KVClient, int) error) {
	var completed, failed int64
	var wg sync.WaitGroup
	stop := make(chan struct{})

	start := time.Now()
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return
			}
			defer conn.Close()
			kc := kvpb.NewKVClient(conn)
			i := worker * 10_000_000
			for {
				select {
				case <-stop:
					return
				default:
				}
				if err := op(kc, i); err != nil {
					atomic.AddInt64(&failed, 1)
				} else {
					atomic.AddInt64(&completed, 1)
				}
				i++
			}
		}(w)
	}
	time.Sleep(duration)
	close(stop)
	wg.Wait()
	elapsed := time.Since(start)

	total := atomic.LoadInt64(&completed)
	fmt.Printf("%d ops in %s -> %.0f ops/sec (%d errored)\n\n", total, elapsed.Round(time.Millisecond), float64(total)/elapsed.Seconds(), atomic.LoadInt64(&failed))
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
