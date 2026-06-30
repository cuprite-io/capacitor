# Capacitor

<p align="center">
  <b>High-voltage synchronization for local-first apps.</b>
</p>

<p align="center">
  <img src="assets/mascot.png" alt="Capacitor Mascot" width="300" />
</p>

<p align="center">
  Capacitor is a distributed, persistent, and sub-millisecond caching layer for the Cuprite Flux engine. It provides high availability and zero-latency local reads/writes by utilizing a local-first architecture synchronized via a background replication log and gossip-based discovery.
</p>

---

## ✨ Key Features

- **Local-First Performance**: All operations (`Get`, `Set`, `Increment`) are performed against a sharded in-memory cache backed by local **BadgerDB** persistence. The network is never on the hot path.
- **Hybrid Logical Clocks (HLC)**: Ensures causality-preserving order for distributed updates without requiring perfect clock synchronization across nodes.
- **Eventual Consistency via CRDTs** (see the [Conflict Resolution Guide](docs/CONFLICT_RESOLUTION.md)):
  - **Registers**: Uses Last-Write-Wins (LWW) based on HLC timestamps.
  - **Counters**: Implements state-based PN-Counters for idempotent increments.
  - **Sliding Windows**: Distributed windowed counters with automated pruning.
- **Asynchronous Replication**: A binary circular **Delta Log** ensures high-throughput, low-latency propagation of updates between peers.
- **Secure by Default**: Supports mTLS for replication streams and shared-secret authentication for the gossip layer.
- **Observability**: Built-in metrics tracking for end-to-end replication latency and operation performance.

## 🏗️ Architecture

Capacitor is designed for high-throughput environments where read/write latency is critical. For a complete deep-dive into internal modules, data flows, and subsystem diagrams, see the [Architecture Guide](docs/ARCHITECTURE.md):

1.  **Write Path**: When a write occurs, it is committed to the sharded in-memory cache, appended to an in-memory binary Delta Log, and asynchronously flushed to the local BadgerDB database.
2.  **Discovery**: Nodes use the **SWIM protocol** (via HashiCorp Memberlist) to discover peers and maintain cluster membership.
3.  **Sync Path**: Background replicators stream entries from the Delta Log to peers over TCP/TLS.
4.  **Conflict Resolution**: Received updates are merged into the local store using HLC-based conflict resolution, ensuring all nodes eventually converge to the same state.

## 📊 Performance

| Operation        | Latency (ns/op) | Latency (ms) |
| :--------------- | :-------------- | :----------- |
| **Set**          | ~226            | 0.00022 ms   |
| **Get**          | ~21             | 0.00002 ms   |
| **Get (Scan)**   | ~29             | 0.00003 ms   |
| **Increment**    | ~409            | 0.00041 ms   |
| **Exists**       | ~19             | 0.00002 ms   |

## 💻 Usage

```go
import "github.com/cuprite-io/capacitor"

cfg := capacitor.Config{
    NodeID:     "node-1",
    BindPort:   7946,           // Gossip port
    StreamPort: 7947,           // Replication port
    Peers:      []string{"10.0.0.1:7946"},
    DataPath:   "/var/lib/capacitor",
    AuthToken:  "your-secure-token",
}

cp, _ := capacitor.New(cfg)
defer cp.Close()

// Standard Cache Operations
cp.Set(ctx, "session:123", "data", 10 * time.Minute)
val, _ := cp.Get(ctx, "session:123")

// Structured Scanning (similar to json.Unmarshal)
var session UserSession
_ = cp.GetScan(ctx, "session:123", &session)

// Distributed Counters
count, _ := cp.Increment(ctx, "page_views")

// Sliding Windows
windowCount, _ := cp.IncrementSlidingWindow(ctx, "rate_limit:user_1", 1 * time.Minute)
```

## 🧪 Testing

Run the comprehensive test suite, including chaos and convergence tests:

```bash
go test -v .
go test -bench=. .
```
