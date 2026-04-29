# Redis Implementation in Go

This is a custom implementation of a Redis server built from scratch using Go. It aims to replicate the core functionality of Redis, handling concurrent client connections and executing standard Redis commands.

## Features

- **Custom TCP Server:** Handles multiple concurrent client connections.
- **RESP (REdis Serialization Protocol) Parser:** Fully implements the protocol to encode and decode messages.
- **Thread-safe In-Memory Store:** Uses Go's concurrency primitives to ensure data integrity across multiple goroutines.
- **Supported Data Structures:**
  - Strings
  - Lists
  - Streams

## Supported Commands

The server currently supports the following subset of Redis commands:

- `PING` / `ECHO` - Connection testing and simple echoing.
- `GET` / `SET` / `INCR` - Basic string manipulation and atomic increments.
- `TYPE` - Returns the type of the value stored at a key.
- `RPUSH` / `LPOP` / `RPOP` / `LLEN` / `LTRIM` / `BLPOP` - List operations, including blocking pops.
- `XADD` / `XRANGE` / `XREAD` - Stream operations, including reading with blocking capabilities.

## Running Locally

1. Ensure you have Go installed on your system.
2. Clone the repository.
3. Start the server using the provided bash script or via `go run`:
   ```sh
   ./your_program.sh
   # OR
   go run app/*.go
   ```
4. Connect to it using `redis-cli` or any other Redis client:
   ```sh
   redis-cli -p 6379
   127.0.0.1:6379> PING
   PONG
   ```
