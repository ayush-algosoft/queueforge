// Package redisrepo hosts two Redis-backed primitives:
//
//  1. A fast deduplication pre-check. Postgres has the authoritative unique
//     index; this just lets the API reject obvious duplicates without a DB
//     roundtrip.
//
//  2. A fixed-window rate limiter for protecting downstream services or
//     enforcing per-tenant submission caps.
package redisrepo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Client wraps go-redis with QueueForge-specific helpers.
type Client struct {
	rdb *redis.Client
}

// New connects to Redis at addr and pings it.
func New(ctx context.Context, addr, password string, db int) (*Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:        addr,
		Password:    password,
		DB:          db,
		DialTimeout: 5 * time.Second,
	})
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		return nil, fmt.Errorf("ping redis: %w", err)
	}
	return &Client{rdb: rdb}, nil
}

// Close releases the underlying connection pool.
func (c *Client) Close() error { return c.rdb.Close() }

// DedupReserve attempts to reserve dedup key for ttl. Returns true if the
// caller "won" the reservation, false if a duplicate already exists.
//
// This is a hint, not a guarantee — the authoritative check is the unique
// index in Postgres. Reserving here only avoids issuing a doomed INSERT.
func (c *Client) DedupReserve(ctx context.Context, queue, key string, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	k := dedupKey(queue, key)
	ok, err := c.rdb.SetNX(ctx, k, "1", ttl).Result()
	if err != nil {
		return false, fmt.Errorf("dedup setnx: %w", err)
	}
	return ok, nil
}

// DedupRelease removes a previously reserved dedup key.
func (c *Client) DedupRelease(ctx context.Context, queue, key string) error {
	return c.rdb.Del(ctx, dedupKey(queue, key)).Err()
}

func dedupKey(queue, key string) string {
	return "qf:dedup:" + queue + ":" + key
}

// RateLimit is a fixed-window counter allowing `capacity` requests per
// `window`. Increment + TTL are set atomically via a Lua script.
//
// Returns (allowed, retryAfter). When allowed is false, retryAfter is the
// time the caller should wait before retrying.
func (c *Client) RateLimit(ctx context.Context, key string, capacity int64, window time.Duration) (bool, time.Duration, error) {
	if capacity <= 0 {
		return true, 0, nil
	}
	res, err := rateLimitScript.Run(ctx, c.rdb, []string{"qf:rl:" + key}, capacity, window.Milliseconds()).Result()
	if err != nil {
		return false, 0, fmt.Errorf("rate limit eval: %w", err)
	}
	arr, ok := res.([]interface{})
	if !ok || len(arr) != 2 {
		return false, 0, errors.New("rate limit: unexpected reply")
	}
	allowed, _ := arr[0].(int64)
	retryMS, _ := arr[1].(int64)
	return allowed == 1, time.Duration(retryMS) * time.Millisecond, nil
}

// Lua: INCR a counter; first write also sets PEXPIRE. If the post-incr value
// exceeds capacity, the request is rejected and we return PTTL as retry-after.
var rateLimitScript = redis.NewScript(`
local current = redis.call('INCR', KEYS[1])
if current == 1 then
    redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
if current > tonumber(ARGV[1]) then
    local pttl = redis.call('PTTL', KEYS[1])
    if pttl < 0 then pttl = tonumber(ARGV[2]) end
    return {0, pttl}
end
return {1, 0}
`)
