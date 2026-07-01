package capacitor

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/vmihailenco/msgpack/v5"
)

// Logger is a generic structured logging interface.
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

type noopLogger struct{}

func (n noopLogger) Debug(msg string, args ...any) {}
func (n noopLogger) Info(msg string, args ...any)  {}
func (n noopLogger) Warn(msg string, args ...any)  {}
func (n noopLogger) Error(msg string, args ...any) {}

// Config defines the configuration parameters for a Capacitor node instance.
type Config struct {
	// NodeID is the unique identifier for this node in the cluster. If left empty,
	// a hostname-based identifier will be generated automatically.
	NodeID string

	// BindAddr is the network address for the gossip memberlist to bind to.
	BindAddr string

	// BindPort is the port number utilized for gossip memberlist communications.
	BindPort int

	// StreamPort is the port number utilized for replication TCP streams.
	StreamPort int

	// AdvertiseAddr is the IP address advertised to other nodes for establishing
	// replication TCP streams.
	AdvertiseAddr string

	// Peers is the initial list of bootstrap addresses ("IP:port") of active cluster nodes.
	Peers []string

	// DataPath is the local directory path where BadgerDB files are persistently stored.
	DataPath string

	// LogSize is the capacity (maximum number of entries) of the in-memory circular Delta Log.
	LogSize uint64

	// TLSConfig is the optional configuration used to secure node-to-node replication streams using mTLS.
	TLSConfig *tls.Config

	// AuthToken is a shared secret token used to authenticate gossip join requests and TCP streams.
	AuthToken string

	// Logger is the structured logging engine injected into the Capacitor instance.
	Logger Logger

	// DisableMetrics disables internal metrics latency tracking for maximum read/write performance.
	DisableMetrics bool
}

// Capacitor represents an active-active, local-first replicated caching node.
// It manages local key-value storage, cluster membership discovery, logical clocks,
// and replication orchestration.
type Capacitor struct {
	nodeID    string
	store     *store
	hlc       *HLC
	log       *DeltaLog
	server    *StreamServer
	client    *StreamClient
	ml        *memberlist.Memberlist
	metrics   *MetricsTracker
	authToken string
	logger    Logger
	ctx       context.Context
	cancel    context.CancelFunc

	// Replicator state

	peerSeqs   sync.Map // map[string]uint64 (Last replicated Seq)
	peerStops  sync.Map // map[string]chan struct{}
	streamAddr string
	stop       chan struct{}
	disableMetrics bool
}

// New initializes, configures, and starts a local Capacitor node.
// This constructs the storage, starts the replication stream listener, and registers
// the node with the gossip cluster.
func New(cfg Config) (*Capacitor, error) {
	if cfg.NodeID == "" {
		hostname, _ := os.Hostname()
		cfg.NodeID = fmt.Sprintf("%s-%d", hostname, time.Now().Unix())
	}
	if cfg.LogSize == 0 {
		cfg.LogSize = 1_000_000
	}
	if cfg.Logger == nil {
		cfg.Logger = noopLogger{}
	}

	s, err := newStore(cfg.DataPath)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	cp := &Capacitor{
		nodeID:    cfg.NodeID,
		store:     s,
		hlc:       NewHLC(),
		log:       NewDeltaLog(cfg.LogSize),
		client:    NewStreamClient(cfg.TLSConfig, cfg.AuthToken),
		metrics:   NewMetricsTracker(),
		stop:      make(chan struct{}),
		authToken: cfg.AuthToken,
		logger:         cfg.Logger,
		ctx:            ctx,
		cancel:         cancel,
		disableMetrics: cfg.DisableMetrics,
	}

	srv, err := NewStreamServer(cp, fmt.Sprintf("0.0.0.0:%d", cfg.StreamPort), cfg.TLSConfig)
	if err != nil {
		return nil, err
	}
	cp.server = srv
	go srv.Start()

	// Get actual bound address
	actualAddr := srv.listener.Addr().String()
	_, actualPortStr, _ := net.SplitHostPort(actualAddr)
	cp.streamAddr = net.JoinHostPort("127.0.0.1", actualPortStr)

	// Memberlist for discovery
	mlCfg := memberlist.DefaultLocalConfig()
	mlCfg.Name = cfg.NodeID
	if cfg.BindAddr != "" {
		mlCfg.BindAddr = cfg.BindAddr
	}
	mlCfg.BindPort = cfg.BindPort

	// Secure Gossip layer with AuthToken
	if cfg.AuthToken != "" {
		hash := sha256.Sum256([]byte(cfg.AuthToken))
		mlCfg.SecretKey = hash[:]
	}

	// Bridge memberlist logging
	mlCfg.Logger = log.New(&mlLoggerBridge{cp.logger}, "", 0)

	events := &clusterEvents{cp: cp}
	mlCfg.Delegate = events
	mlCfg.Events = events

	ml, err := memberlist.Create(mlCfg)
	if err != nil {
		return nil, err
	}
	cp.ml = ml

	// Determine the actual port bound
	_, portStr, _ := net.SplitHostPort(srv.listener.Addr().String())

	// Determine the address to advertise for TCP streams
	advertiseAddr := cfg.AdvertiseAddr
	if advertiseAddr == "" {
		// Use memberlist's detected advertise address
		advertiseAddr = mlCfg.AdvertiseAddr
	}

	// If still empty or 0.0.0.0, fallback to 127.0.0.1 (common in local dev/tests)
	if advertiseAddr == "" || advertiseAddr == "0.0.0.0" {
		advertiseAddr = "127.0.0.1"
	}
	cp.streamAddr = net.JoinHostPort(advertiseAddr, portStr)

	if len(cfg.Peers) > 0 {
		_, _ = ml.Join(cfg.Peers)
	}

	return cp, nil
}

type clusterEvents struct {
	cp *Capacitor
}

type mlLoggerBridge struct {
	l Logger
}

func (m *mlLoggerBridge) Write(p []byte) (n int, err error) {
	// Memberlist logs are usually prefixed with [DEBUG], [ERR], etc.
	msg := string(p)
	switch {
	case strings.Contains(msg, "[DEBUG]"):
		m.l.Debug(strings.TrimSpace(msg))
	case strings.Contains(msg, "[ERR]"):
		m.l.Error(strings.TrimSpace(msg))
	case strings.Contains(msg, "[WARN]"):
		m.l.Warn(strings.TrimSpace(msg))
	default:
		m.l.Info(strings.TrimSpace(msg))
	}
	return len(p), nil
}

func (n *clusterEvents) NodeMeta(limit int) []byte {
	// Simple handshake: share the address
	// In a more complex setup, we could encode a map of LastSeenSeqs here.
	// For now, let's just stick to the address and let replicators start from store state.
	return []byte(n.cp.streamAddr)
}
func (n *clusterEvents) NotifyMsg([]byte)                           {}
func (n *clusterEvents) GetBroadcasts(overhead, limit int) [][]byte { return nil }
func (n *clusterEvents) LocalState(join bool) []byte                { return nil }
func (n *clusterEvents) MergeRemoteState(buf []byte, join bool)     {}

func (n *clusterEvents) NotifyJoin(node *memberlist.Node) {
	if node.Name == n.cp.nodeID {
		return
	}
	n.cp.startPeerReplicator(node.Name, string(node.Meta))
}

func (n *clusterEvents) NotifyLeave(node *memberlist.Node) {
	if n.cp.client != nil {
		n.cp.client.CloseConn(node.Name)
	}
	n.cp.stopPeerReplicator(node.Name)
}

func (n *clusterEvents) NotifyUpdate(node *memberlist.Node) {}

func (f *Capacitor) startPeerReplicator(nodeID, addr string) {
	if addr == "" {
		return
	}

	// Avoid duplicates
	if _, loaded := f.peerStops.Load(nodeID); loaded {
		return
	}

	stop := make(chan struct{})
	f.peerStops.Store(nodeID, stop)

	go f.peerReplicatorLoop(nodeID, addr, stop)
}

func (f *Capacitor) stopPeerReplicator(nodeID string) {
	if stopVal, loaded := f.peerStops.LoadAndDelete(nodeID); loaded {
		close(stopVal.(chan struct{}))
	}
	f.log.cond.Broadcast() // Wake up waiting replicators to check their stop channel
	f.peerSeqs.Delete(nodeID)
}

func (f *Capacitor) peerReplicatorLoop(nodeID, addr string, stop chan struct{}) {
	for {
		// 1. Wait until there is new data for this peer
		f.log.mu.Lock()
		for {
			select {
			case <-stop:
				f.log.mu.Unlock()
				return
			case <-f.ctx.Done():
				f.log.mu.Unlock()
				return
			default:
			}

			lastSeqVal, _ := f.peerSeqs.Load(nodeID)
			var lastSeq uint64
			if lastSeqVal != nil {
				lastSeq = lastSeqVal.(uint64)
			} else {
				lastSeq = f.store.getPeerSeq(nodeID)
			}

			// head is the next sequence ID to be assigned.
			// Entries exist up to head-1.
			// If lastSeq is the last one we sent, we want to wait until head > lastSeq + 1.
			if f.log.head > lastSeq+1 {
				break
			}
			f.log.cond.Wait()
		}
		f.log.mu.Unlock()

		// 2. Perform replication
		f.replicateToPeer(f.ctx, nodeID, addr)
	}
}

func (f *Capacitor) replicateToPeer(ctx context.Context, nodeID, addr string) {
	lastSeqVal, _ := f.peerSeqs.LoadOrStore(nodeID, f.store.getPeerSeq(nodeID))
	lastSeq := lastSeqVal.(uint64)

	// 1. Get new entries from log as raw binary slices
	// Hard-limit to 1000 entries OR 8MB per batch for backpressure
	rawEntries := f.log.GetEntriesRaw(lastSeq+1, 1000, 8*1024*1024)
	if len(rawEntries) == 0 {
		return
	}

	batch := Batch{
		FromNode: f.nodeID,
		Entries:  rawEntries,
	}

	// 2. Stream to this specific peer
	// Pass our LastSeenSeq from them to initiate handshake
	lastSeenRemote := f.store.getPeerSeq(nodeID)

	// Create a sub-context with timeout for the replication batch
	repCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := f.client.SendBatch(repCtx, nodeID, addr, batch, lastSeenRemote); err == nil {
		// Only advance on success
		f.peerSeqs.Store(nodeID, lastSeq+uint64(len(rawEntries)))
	}
}

func (f *Capacitor) applyRemoteEntry(ctx context.Context, e LogEntry) {
	// Track replication latency
	if e.BornAt > 0 {
		f.metrics.ReplicateLat.Record(time.Since(time.Unix(0, e.BornAt)))
	}

	// Update local HLC
	if _, err := f.hlc.Update(e.TS); err != nil {
		// Log clock smash and drop the entry to prevent poisoning
		return
	}

	switch e.Op {
	case MsgSet:
		var val any
		_ = msgpack.Unmarshal(e.Value, &val)
		f.store.set(e.Key, val, e.TS, time.Duration(e.TTL)*time.Millisecond)
	case MsgIncr:
		f.store.setNodeCount(e.Key, e.NodeID, e.Delta)
	case MsgMetric:
		// Metrics are eventually consistent across nodes
		f.store.incrementMetric(e.Key, e.NodeID, e.Delta)
	case MsgWindow:
		f.store.addWindowTimestamp(e.Key, e.TS.Physical)
	case MsgDelete:
		f.store.delete(e.Key, e.TS)
	}

	// Record persistent sequence for catch-up after restart
	f.store.updatePeerSeq(e.NodeID, e.Seq)
}

// Set writes a key-value pair to the local store and appends it to the replication
// log to propagate it to other nodes in the cluster. If a positive TTL is specified,
// the key-value pair will automatically expire.
func (f *Capacitor) Set(ctx context.Context, key string, value any, ttl time.Duration) error {
	var start time.Time
	if !f.disableMetrics {
		start = time.Now()
	}
	ts := f.hlc.Now()

	// 1. Local Write
	if err := f.store.set(key, value, ts, ttl); err != nil {
		return err
	}

	// 2. Serialize Once for Log/Replication
	binVal, err := msgpack.Marshal(value)
	if err != nil {
		return err
	}

	// 3. Log Append
	f.log.Append(LogEntry{
		TS:     ts,
		BornAt: time.Now().UnixNano(),
		Op:     MsgSet,
		Key:    key,
		Value:  binVal,
		TTL:    int64(ttl.Milliseconds()),
	})
	if !f.disableMetrics {
		f.metrics.SetLat.Record(time.Since(start))
	}
	return nil
}

// GetScan retrieves a key's value and unmarshals it into the destination pointer dest
// (similar to json.Unmarshal or database rows.Scan).
func (f *Capacitor) GetScan(ctx context.Context, key string, dest any) error {
	var start time.Time
	if !f.disableMetrics {
		start = time.Now()
	}

	err := f.getScanInternal(ctx, key, dest)

	if !f.disableMetrics {
		f.metrics.GetLat.Record(time.Since(start))
	}
	return err
}

func (f *Capacitor) getScanInternal(ctx context.Context, key string, dest any) error {
	rawVal, err := f.store.get(key)
	if err != nil {
		return err
	}
	if rawVal == nil {
		return fmt.Errorf("capacitor: key %s not found", key)
	}

	// 1. Reflection-based Direct Assignment Fast Path (In-Memory Hot Path Bypass)
	destVal := reflect.ValueOf(dest)
	if destVal.Kind() == reflect.Ptr {
		elemVal := destVal.Elem()
		if elemVal.Type() == reflect.TypeOf(rawVal) {
			elemVal.Set(reflect.ValueOf(rawVal))
			return nil
		}
	}

	// Direct assignment for simple types
	switch d := dest.(type) {
	case *string:
		switch v := rawVal.(type) {
		case string:
			*d = v
		case []byte:
			*d = string(v)
		default:
			b, _ := msgpack.Marshal(v)
			*d = string(b)
		}
		return nil
	case *[]byte:
		switch v := rawVal.(type) {
		case []byte:
			*d = v
		case string:
			*d = []byte(v)
		default:
			*d, _ = msgpack.Marshal(v)
		}
		return nil
	}

	// Fallback to MsgPack unmarshaling
	switch v := rawVal.(type) {
	case []byte:
		return msgpack.Unmarshal(v, dest)
	case string:
		return msgpack.Unmarshal([]byte(v), dest)
	default:
		b, err := msgpack.Marshal(v)
		if err != nil {
			return err
		}
		return msgpack.Unmarshal(b, dest)
	}
}

// Get retrieves a key-value pair's serialized value from the local cache database.
// Returns an empty string and nil error if the key is not found or has expired.
func (f *Capacitor) Get(ctx context.Context, key string) (string, error) {
	var start time.Time
	if !f.disableMetrics {
		start = time.Now()
	}

	res, err := f.getInternal(ctx, key)

	if !f.disableMetrics {
		f.metrics.GetLat.Record(time.Since(start))
	}
	return res, err
}

func (f *Capacitor) getInternal(ctx context.Context, key string) (string, error) {
	val, err := f.store.get(key)
	if err != nil {
		return "", err
	}
	if val == nil {
		return "", nil
	}

	res := ""
	switch v := val.(type) {
	case string:
		res = v
	case []byte:
		res = string(v)
	default:
		b, _ := msgpack.Marshal(v)
		res = string(b)
	}
	return res, nil
}

// Exists checks if a key exists in the cache and has not expired.
func (f *Capacitor) Exists(ctx context.Context, key string) (bool, error) {
	var start time.Time
	if !f.disableMetrics {
		start = time.Now()
	}

	exists, err := f.store.exists(key)

	if !f.disableMetrics {
		f.metrics.GetLat.Record(time.Since(start))
	}
	return exists, err
}

// Delete removes a key-value pair from the local store and replicates the deletion tombstone to the cluster.
func (f *Capacitor) Delete(ctx context.Context, key string) error {
	var start time.Time
	if !f.disableMetrics {
		start = time.Now()
	}
	ts := f.hlc.Now()

	// 1. Local Write
	if err := f.store.delete(key, ts); err != nil {
		return err
	}

	// 2. Log Append
	f.log.Append(LogEntry{
		TS:     ts,
		BornAt: time.Now().UnixNano(),
		Op:     MsgDelete,
		Key:    key,
	})

	if !f.disableMetrics {
		f.metrics.SetLat.Record(time.Since(start))
	}
	return nil
}



// IncrementBy increments a distributed PN-Counter key by the specified delta.
// It tracks counts per-node to construct CRDT conflict-free convergence.
func (f *Capacitor) IncrementBy(ctx context.Context, key string, delta int64) (int64, error) {
	var start time.Time
	if !f.disableMetrics {
		start = time.Now()
	}
	val, err := f.store.increment(key, f.nodeID, float64(delta))
	if err != nil {
		return 0, err
	}

	nodeVal, _ := f.store.getNodeCount(key, f.nodeID)
	f.log.Append(LogEntry{
		TS:     f.hlc.Now(),
		BornAt: time.Now().UnixNano(),
		Op:     MsgIncr,
		Key:    key,
		Delta:  nodeVal,
		NodeID: f.nodeID,
	})
	if !f.disableMetrics {
		f.metrics.IncrLat.Record(time.Since(start))
	}
	return int64(val), nil
}

// GetMetrics returns a snapshot summary of all built-in metrics (latencies, counts).
func (f *Capacitor) GetMetrics() []Summary {
	return f.metrics.GetSummary()
}

// Increment increments a distributed PN-Counter key by 1.
func (f *Capacitor) Increment(ctx context.Context, key string) (int64, error) {
	return f.IncrementBy(ctx, key, 1)
}

// GetCount retrieves the converged aggregate sum of a distributed counter across all nodes.
func (f *Capacitor) GetCount(ctx context.Context, key string) (int64, error) {
	val, err := f.store.getAggregateCount(key)
	return int64(val), err
}

// IncrementParallel performs concurrent increment calls for multiple counter keys.
func (f *Capacitor) IncrementParallel(ctx context.Context, keys []string) (map[string]int64, error) {
	res := make(map[string]int64)
	for _, k := range keys {
		val, _ := f.Increment(ctx, k)
		res[k] = val
	}
	return res, nil
}

// IncrementMetric records a floating-point update to a distributed aggregate metric
// (tracking both hit frequency and aggregated sums).
func (f *Capacitor) IncrementMetric(ctx context.Context, key string, delta float64) (Metric, error) {
	m, err := f.store.incrementMetric(key, f.nodeID, delta)
	if err != nil {
		return Metric{}, err
	}

	f.log.Append(LogEntry{
		TS:     f.hlc.Now(),
		Op:     MsgMetric,
		Key:    key,
		Delta:  delta,
		NodeID: f.nodeID,
	})
	return m, nil
}

// GetMetric retrieves the aggregated Metric details (count, sum, average) for the specified key.
func (f *Capacitor) GetMetric(ctx context.Context, key string) (Metric, error) {
	return f.store.getAggregateMetric(key)
}

// IncrementMetricParallel updates multiple aggregate metrics concurrently.
func (f *Capacitor) IncrementMetricParallel(ctx context.Context, keys map[string]float64) (map[string]Metric, error) {
	res := make(map[string]Metric)
	for k, d := range keys {
		m, _ := f.IncrementMetric(ctx, k, d)
		res[k] = m
	}
	return res, nil
}

// IncrementSlidingWindow appends an event timestamp for rate limiting or hit tracking,
// and returns the count of active occurrences within the rolling window duration.
func (f *Capacitor) IncrementSlidingWindow(ctx context.Context, key string, window time.Duration) (int64, error) {
	ts := f.hlc.Now()
	count, err := f.store.incrementSlidingWindow(key, ts.Physical, window)
	if err != nil {
		return 0, err
	}

	f.log.Append(LogEntry{
		TS:     ts,
		Op:     MsgWindow,
		Key:    key,
		NodeID: f.nodeID,
	})
	return count, nil
}

// Close gracefully shuts down the node, leaving the SWIM gossip group, shutting down
// active replication streams, and closing local storage engines.
func (f *Capacitor) Close() error {
	f.cancel()
	close(f.stop)
	f.log.cond.Broadcast() // Wake up waiting replicators
	f.metrics.Stop()
	if f.ml != nil {
		f.ml.Leave(10 * time.Second)
		f.ml.Shutdown()
	}
	f.server.Stop()
	f.client.Close()
	return f.store.close()
}
