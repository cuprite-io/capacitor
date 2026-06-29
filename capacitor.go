package capacitor

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"os"
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

type Config struct {
	NodeID        string
	BindAddr      string
	BindPort      int    // Gossip port
	StreamPort    int    // TCP Stream port
	AdvertiseAddr string // NEW: Address to advertise to other nodes for TCP streams
	Peers         []string
	DataPath      string
	LogSize       uint64
	TLSConfig     *tls.Config // NEW: Optional TLS configuration for mTLS
	AuthToken     string      // NEW: Shared secret for cluster authentication
	Logger        Logger      // NEW: Injectable structured logger
}

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
}

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
		logger:    cfg.Logger,
		ctx:       ctx,
		cancel:    cancel,
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
		f.store.set(e.Key, val, e.TS.Physical, time.Duration(e.TTL)*time.Second)
	case MsgIncr:
		f.store.setNodeCount(e.Key, e.NodeID, e.Delta)
	case MsgMetric:
		// Metrics are eventually consistent across nodes
		f.store.incrementMetric(e.Key, e.NodeID, e.Delta)
	}

	// Record persistent sequence for catch-up after restart
	f.store.updatePeerSeq(e.NodeID, e.Seq)
}

// CacheRepository implementation

func (f *Capacitor) Set(ctx context.Context, key string, value any, ttl time.Duration) error {
	start := time.Now()
	ts := f.hlc.Now()

	// 1. Local Write
	if err := f.store.set(key, value, ts.Physical, ttl); err != nil {
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
		TTL:    int64(ttl.Seconds()),
	})
	f.metrics.SetLat.Record(time.Since(start))
	return nil
}

func (f *Capacitor) Get(ctx context.Context, key string) (string, error) {
	start := time.Now()
	val, err := f.store.get(key)
	if err != nil {
		return "", err
	}
	if val == nil {
		f.metrics.GetLat.Record(time.Since(start))
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
	f.metrics.GetLat.Record(time.Since(start))
	return res, nil
}

func (f *Capacitor) IncrementBy(ctx context.Context, key string, delta int64) (int64, error) {
	start := time.Now()
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
	f.metrics.IncrLat.Record(time.Since(start))
	return int64(val), nil
}

func (f *Capacitor) GetMetrics() []Summary {
	return f.metrics.GetSummary()
}

func (f *Capacitor) Increment(ctx context.Context, key string) (int64, error) {
	return f.IncrementBy(ctx, key, 1)
}

func (f *Capacitor) GetCount(ctx context.Context, key string) (int64, error) {
	val, err := f.store.getAggregateCount(key)
	return int64(val), err
}

func (f *Capacitor) IncrementParallel(ctx context.Context, keys []string) (map[string]int64, error) {
	res := make(map[string]int64)
	for _, k := range keys {
		val, _ := f.Increment(ctx, k)
		res[k] = val
	}
	return res, nil
}

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

func (f *Capacitor) GetMetric(ctx context.Context, key string) (Metric, error) {
	return f.store.getAggregateMetric(key)
}

func (f *Capacitor) IncrementMetricParallel(ctx context.Context, keys map[string]float64) (map[string]Metric, error) {
	res := make(map[string]Metric)
	for k, d := range keys {
		m, _ := f.IncrementMetric(ctx, k, d)
		res[k] = m
	}
	return res, nil
}

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
