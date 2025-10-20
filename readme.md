# Blueis

Blueis is a simple in-memory key-value store server inspired by Redis, written in Go. It supports string and hash operations, key expiration, and persistence using an Append Only File (AOF).

## Features

- RESP protocol support (subset)
- Commands: `PING`, `SET`, `GET`, `HSET`, `HGET`, `HGETALL`, `EXPIRE`, `TTL`, `ZADD`, `ZRANGE`, `ZREM`
- Key expiration with background cleanup
- Hash data structure support
- **Sorted Set (ZSET) support using skiplists for efficient range queries and ordering**
- AOF-based persistence and replay on startup
- Concurrent client handling

## Project Structure

- [`main.go`](main.go): Entry point, TCP server, connection handling, AOF replay on startup.
- [`handler.go`](handler.go): Command handlers, in-memory data structures, expiration logic.
- [`resp.go`](resp.go): RESP protocol parsing and serialization.
- [`aof.go`](aof.go): Append Only File persistence logic.
- `.gitignore`: Ignores `.aof` files.
- `go.mod`: Go module definition.
- `database.aof`: The AOF file (created at runtime).

## Usage

1. **Build and Run**

   ```sh
   go run main.go
   ```

   The server listens on port `6379` by default.

2. **Connect with redis-cli**

   You can use `redis-cli` or any TCP client that speaks the RESP protocol:

   ```sh
   redis-cli -p 6379
   ```

   Example commands:

   ```
   SET mykey hello
   GET mykey
   HSET myhash field1 value1
   HGET myhash field1
   EXPIRE mykey 10
   TTL mykey
   ZADD myzset 1 one 2 two 3 three
   ZRANGE myzset 0 -1 WITHSCORES
   ZREM myzset two
   ```

## Persistence

- All write operations (`SET`, `HSET`, `EXPIRE`, `ZADD`) are logged to `database.aof`.
- On startup, the server replays the AOF to restore the previous state.

## Notes

- Only a subset of Redis commands and types are supported.
- Expired keys are cleaned up on access or when TTL/GET/HGET/HGETALL is called.

## ZSET and Skiplist Implementation

- **ZSETs (sorted sets) are implemented using skiplists** via the [`sortedset`](https://github.com/wangjia184/sortedset) Go package.
- Skiplists provide efficient O(log N) insertion, deletion, and range query operations, making them ideal for sorted set operations like `ZADD`, `ZRANGE`, and `ZREM`.
- This design ensures that range queries and ordered access to elements in a ZSET are fast and scalable, similar to how Redis implements sorted sets.

