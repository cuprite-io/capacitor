# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-06-06

### Added
- **First working model of Capacitor**: A distributed, local-first caching layer.
- **Hybrid Logical Clocks (HLC)**: Implementation for causality-preserving event ordering.
- **Conflict-free Replicated Data Types (CRDT)**: Support for LWW-Registers and PN-Counters.
- **Delta Log Replication**: Binary circular buffer for high-throughput asynchronous synchronization.
- **Secure Transport**: mTLS support for replication streams and authenticated gossip discovery.
- **Observability**: Integrated metrics for tracking replication latency and store performance.
- **Persistence**: Integration with BadgerDB for sharded, high-performance local storage.
```
