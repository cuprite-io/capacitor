# Conflict Resolution Guide

Capacitor ensures eventual consistency across a distributed cluster without a central coordinator or leader. It achieves this using two primary mechanisms: **Hybrid Logical Clocks (HLC)** and **Conflict-free Replicated Data Types (CRDT)**.

## 1. Hybrid Logical Clocks (HLC)

Standard wall clocks (NTP) can drift or move backward, making them unreliable for ordering events in a distributed system. Capacitor uses HLCs to provide:
- **Causality Tracking**: If Event A happened before Event B on the same node, A's timestamp will be strictly less than B's.
- **Clock Drift Tolerance**: HLCs stay close to physical time but can "tick" logically if the physical clock lags behind the cluster's perceived time.

### Last-Write-Wins (LWW)
For simple key-value pairs (`Set` operations), Capacitor uses LWW. When two nodes write to the same key, the update with the higher HLC timestamp wins. Because HLCs are monotonic and causal, the cluster eventually converges on the same value.

## 2. Distributed Counters (PN-Counters)

For the `Increment` operation, LWW is insufficient (two nodes incrementing at the same time would lose one increment). 

Capacitor implements **State-based PN-Counters**:
- Each node maintains its own partial counter for every key.
- Nodes gossip their absolute partial counts to peers.
- The total count for a key is the sum of all known partial counts from all nodes.
- This ensures that increments are idempotent and commutative; no matter what order the updates arrive, the final sum is the same.

## 3. Sliding Windows

Sliding window counters work by replicating individual event timestamps.
- Each node records its local events with an HLC timestamp.
- These timestamps are gossiped and merged into a local "window" of events.
- Nodes periodically "prune" timestamps that have fallen outside the window (e.g., older than 1 minute).
- This allows for distributed rate-limiting and frequency tracking with high accuracy.

## 4. Clock Smash Protection

To prevent a node with a wildly incorrect clock from poisoning the cluster, Capacitor implements **Clock Smash Protection**. Any incoming update with a timestamp too far in the future (default: 500ms) compared to the local wall clock is rejected.
```
