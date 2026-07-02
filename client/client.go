package client

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Atharva9890/raft-kv-store/proto/kvpb"
)

// a minimal KV client that doesn't know or care which node is
// currently the leader. every call tries whichever node it last had
// success with; on a NotLeader reply it follows the LeaderHint if the
// server gave one, otherwise it just moves down the list until
// something answers for real.
//
// this "try, get redirected, retry" dance is the whole client-side
// story for staying available through an election - no service
// discovery, no external coordinator, just retry against the cluster
// you were handed.
type Client struct {
	addrs      []string
	leaderIdx  int
	timeout    time.Duration
	maxRetries int
}

func New(addrs []string) *Client {
	return &Client{addrs: addrs, timeout: 2 * time.Second, maxRetries: len(addrs) * 2}
}

func (c *Client) dial(addr string) (kvpb.KVClient, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	return kvpb.NewKVClient(conn), conn, nil
}

func (c *Client) Get(key string) (string, bool, error) {
	var value string
	var found bool
	err := c.withRetry(func(kc kvpb.KVClient, ctx context.Context) (bool, string, error) {
		resp, err := kc.Get(ctx, &kvpb.GetRequest{Key: key})
		if err != nil {
			return false, "", err
		}
		if resp.NotLeader {
			return false, resp.LeaderHint, nil
		}
		value, found = resp.Value, resp.Found
		return true, "", nil
	})
	return value, found, err
}

func (c *Client) Put(key, value string) error {
	return c.withRetry(func(kc kvpb.KVClient, ctx context.Context) (bool, string, error) {
		resp, err := kc.Put(ctx, &kvpb.PutRequest{Key: key, Value: value})
		if err != nil {
			return false, "", err
		}
		if resp.NotLeader {
			return false, resp.LeaderHint, nil
		}
		return true, "", nil
	})
}

func (c *Client) Delete(key string) error {
	return c.withRetry(func(kc kvpb.KVClient, ctx context.Context) (bool, string, error) {
		resp, err := kc.Delete(ctx, &kvpb.DeleteRequest{Key: key})
		if err != nil {
			return false, "", err
		}
		if resp.NotLeader {
			return false, resp.LeaderHint, nil
		}
		return true, "", nil
	})
}

// tries call against my current best guess at the leader. if call
// reports NotLeader, I move to the hinted address (when the node
// bothered to tell me who it thinks the leader is) or just the next
// node in the list, then try again - this is what makes "kill the
// leader, client keeps working" true without the caller changing
// anything.
func (c *Client) withRetry(call func(kvpb.KVClient, context.Context) (ok bool, hint string, err error)) error {
	var lastErr error
	for attempt := 0; attempt < c.maxRetries; attempt++ {
		addr := c.addrs[c.leaderIdx%len(c.addrs)]
		kc, conn, err := c.dial(addr)
		if err != nil {
			lastErr = err
			c.leaderIdx++
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
		ok, hint, err := call(kc, ctx)
		cancel()
		_ = conn.Close()

		if err != nil {
			lastErr = err
			c.leaderIdx++
			continue
		}
		if ok {
			return nil
		}
		lastErr = fmt.Errorf("%s is not the leader", addr)
		if hint != "" {
			for i, a := range c.addrs {
				if a == hint {
					c.leaderIdx = i
				}
			}
		} else {
			c.leaderIdx++
		}
	}
	return fmt.Errorf("giving up after %d attempts: %w", c.maxRetries, lastErr)
}
