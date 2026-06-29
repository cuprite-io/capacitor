package capacitor

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/vmihailenco/msgpack/v5"
)

const numShards = 256

type storeShard struct {
	mu              sync.RWMutex
	cache           map[string]valWithTS
	counts          map[string]float64
	aggregateCounts map[string]float64
	windows         map[string][]int64
	dirty           map[string]bool
	peerSeqs        map[string]uint64
}

func newStoreShard() *storeShard {
	return &storeShard{
		cache:           make(map[string]valWithTS),
		counts:          make(map[string]float64),
		aggregateCounts: make(map[string]float64),
		windows:         make(map[string][]int64),
		dirty:           make(map[string]bool),
		peerSeqs:        make(map[string]uint64),
	}
}

type store struct {
	db     *badger.DB
	shards [numShards]*storeShard
	stop   chan struct{}
}

func getShardKey(key string) string {
	first := strings.Index(key, ":")
	if first == -1 {
		return key
	}
	second := strings.Index(key[first+1:], ":")
	if second == -1 {
		return key[first+1:]
	}
	return key[first+1 : first+1+second]
}

func hash(s string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

func (s *store) getShard(key string) *storeShard {
	return s.shards[hash(getShardKey(key))%numShards]
}

func newStore(path string) (*store, error) {
	opts := badger.DefaultOptions(path).
		WithLogger(nil).
		WithInMemory(path == "")

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	s := &store{
		db:   db,
		stop: make(chan struct{}),
	}
	for i := 0; i < numShards; i++ {
		s.shards[i] = newStoreShard()
	}

	if err := s.load(); err != nil {
		db.Close()
		return nil, err
	}

	go s.flushLoop()
	go s.janitorLoop()

	return s, nil
}

func (s *store) load() error {
	return s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(nil); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key())
			_ = item.Value(func(val []byte) error {
				shard := s.getShard(key)

				// peer sequence numbers
				if strings.HasPrefix(key, "meta:seq:") {
					nodeID := key[9:] // Skip "meta:seq:"
					shard.peerSeqs[nodeID] = binary.BigEndian.Uint64(val)
					return nil
				}

				// distributed counters
				if strings.HasPrefix(key, "c:") {
					v := float64FromBytes(val)
					shard.counts[key] = v
					// Key format: c:keyName:nodeID
					parts := strings.Split(key, ":")
					if len(parts) >= 3 {
						kStr := parts[1]
						shard.aggregateCounts[kStr] += v
					}
					return nil
				}

				// metrics count
				if strings.HasPrefix(key, "mc:") {
					v := float64FromBytes(val)
					shard.counts[key] = v
					// Key format: mc:keyName:nodeID
					parts := strings.Split(key, ":")
					if len(parts) >= 3 {
						kStr := parts[1]
						shard.aggregateCounts["m:c:"+kStr] += v
					}
					return nil
				}

				// metrics sum
				if strings.HasPrefix(key, "ms:") {
					v := float64FromBytes(val)
					shard.counts[key] = v
					// Key format: ms:keyName:nodeID
					parts := strings.Split(key, ":")
					if len(parts) >= 3 {
						kStr := parts[1]
						shard.aggregateCounts["m:s:"+kStr] += v
					}
					return nil
				}

				// sliding window
				if strings.HasPrefix(key, "sw:") {
					// Key format: sw:keyName:timestamp
					parts := strings.Split(key, ":")
					if len(parts) >= 3 {
						kStr := parts[1]
						ts, _ := strconv.ParseInt(parts[2], 10, 64)
						shard.windows[kStr] = append(shard.windows[kStr], ts)
					}
					return nil
				}

				var vTS valWithTS
				if err := msgpack.Unmarshal(val, &vTS); err == nil {
					shard.cache[key] = vTS
				}
				return nil
			})
		}
		return nil
	})
}

func (s *store) flushLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			s.flush()
		case <-s.stop:
			ticker.Stop()
			return
		}
	}
}

func (s *store) janitorLoop() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.pruneStaleWindows()
		case <-s.stop:
			return
		}
	}
}

func (s *store) pruneStaleWindows() {
	now := time.Now().UnixNano()
	// Stale threshold: 1 hour
	threshold := int64(time.Hour)

	for i := 0; i < numShards; i++ {
		shard := s.shards[i]
		shard.mu.Lock()
		for key, win := range shard.windows {
			if len(win) == 0 {
				delete(shard.windows, key)
				continue
			}
			// If newest timestamp is older than threshold, delete the window
			if now-win[len(win)-1] > threshold {
				delete(shard.windows, key)
			}
		}
		shard.mu.Unlock()
	}
}

func (s *store) flush() {
	wb := s.db.NewWriteBatch()
	defer wb.Cancel()

	for i := 0; i < numShards; i++ {
		shard := s.shards[i]
		shard.mu.Lock()
		if len(shard.dirty) == 0 {
			shard.mu.Unlock()
			continue
		}

		// Snapshot dirty keys and values under lock
		type snapshotItem struct {
			key   string
			val   any
			count float64
			win   []int64
		}
		items := make([]snapshotItem, 0, len(shard.dirty))

		oldDirty := shard.dirty
		shard.dirty = make(map[string]bool)

		for k := range oldDirty {
			item := snapshotItem{key: k}
			if v, ok := shard.cache[k]; ok {
				item.val = v
			} else if c, ok := shard.counts[k]; ok {
				item.count = c
			} else if w, ok := shard.windows[k]; ok {
				// Copy window slice to avoid race if it's pruned later
				item.win = append([]int64(nil), w...)
			} else if seq, ok := shard.peerSeqs[k]; ok {
				item.count = float64(seq) // Borrow count field for seq
			}
			items = append(items, item)
		}
		shard.mu.Unlock()

		// Serialize and add to WriteBatch outside of shard lock
		for _, item := range items {
			if strings.HasPrefix(item.key, "meta:seq:") {
				seqBuf := make([]byte, 8)
				binary.BigEndian.PutUint64(seqBuf, uint64(item.count))
				_ = wb.Set([]byte(item.key), seqBuf)
				continue
			}
			if item.val != nil {
				data, _ := msgpack.Marshal(item.val)
				_ = wb.Set([]byte(item.key), data)
			} else if item.win != nil {
				for _, ts := range item.win {
					wKey := fmt.Sprintf("sw:%s:%d", item.key, ts)
					_ = wb.Set([]byte(wKey), []byte{})
				}
			} else {
				// Must be a counter
				_ = wb.Set([]byte(item.key), float64ToBytes(item.count))
			}
		}
	}
	_ = wb.Flush()
}

type valWithTS struct {
	Value     any   `msgpack:"v"`
	Timestamp int64 `msgpack:"t"`
}

func (s *store) set(key string, value any, ts int64, ttl time.Duration) error {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if existing, ok := shard.cache[key]; ok && existing.Timestamp >= ts {
		return nil
	}

	shard.cache[key] = valWithTS{Value: value, Timestamp: ts}
	shard.dirty[key] = true
	return nil
}

func (s *store) get(key string) (any, error) {
	shard := s.getShard(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	if vTS, ok := shard.cache[key]; ok {
		return vTS.Value, nil
	}
	return nil, nil
}

func (s *store) increment(key string, nodeID string, delta float64) (float64, error) {
	nodeKey := fmt.Sprintf("c:%s:%s", key, nodeID)
	shard := s.getShard(key)

	shard.mu.Lock()
	shard.counts[nodeKey] += delta
	shard.aggregateCounts[key] += delta
	shard.dirty[nodeKey] = true
	agg := shard.aggregateCounts[key]
	shard.mu.Unlock()

	return agg, nil
}

func (s *store) CacheSize() int {
	var total int
	for i := 0; i < numShards; i++ {
		shard := s.shards[i]
		shard.mu.RLock()
		total += len(shard.cache)
		shard.mu.RUnlock()
	}
	return total
}

func (s *store) getNodeCount(key string, nodeID string) (float64, error) {
	nodeKey := fmt.Sprintf("c:%s:%s", key, nodeID)
	shard := s.getShard(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	return shard.counts[nodeKey], nil
}

func (s *store) setNodeCount(key string, nodeID string, val float64) error {
	nodeKey := fmt.Sprintf("c:%s:%s", key, nodeID)
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if val > shard.counts[nodeKey] {
		diff := val - shard.counts[nodeKey]
		shard.counts[nodeKey] = val
		shard.aggregateCounts[key] += diff
		shard.dirty[nodeKey] = true
	}
	return nil
}

func (s *store) getAggregateCount(key string) (float64, error) {
	shard := s.getShard(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	return shard.aggregateCounts[key], nil
}

func (s *store) incrementMetric(key string, nodeID string, delta float64) (Metric, error) {
	cKey := fmt.Sprintf("mc:%s:%s", key, nodeID)
	sKey := fmt.Sprintf("ms:%s:%s", key, nodeID)

	shard := s.getShard(key)

	shard.mu.Lock()
	shard.counts[cKey] += 1
	shard.aggregateCounts["m:c:"+key] += 1
	shard.dirty[cKey] = true

	shard.counts[sKey] += delta
	shard.aggregateCounts["m:s:"+key] += delta
	shard.dirty[sKey] = true

	m := Metric{
		Count: int64(shard.aggregateCounts["m:c:"+key]),
		Sum:   shard.aggregateCounts["m:s:"+key],
	}
	shard.mu.Unlock()

	return m, nil
}

func (s *store) getAggregateMetric(key string) (Metric, error) {
	shard := s.getShard(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	return Metric{
		Count: int64(shard.aggregateCounts["m:c:"+key]),
		Sum:   shard.aggregateCounts["m:s:"+key],
	}, nil
}

func (s *store) incrementSlidingWindow(key string, ts int64, window time.Duration) (int64, error) {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	shard.windows[key] = append(shard.windows[key], ts)
	shard.dirty[key] = true

	now := time.Now().UnixNano()
	horizon := now - window.Nanoseconds()

	var count int64
	var i int
	for _, t := range shard.windows[key] {
		if t >= horizon {
			count++
		} else {
			i++
		}
	}
	if i > 0 {
		shard.windows[key] = shard.windows[key][i:]
	}

	if len(shard.windows[key]) == 0 {
		delete(shard.windows, key)
	}

	return count, nil
}

func float64ToBytes(f float64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], math.Float64bits(f))
	return buf[:]
}

func float64FromBytes(b []byte) float64 {
	if len(b) < 8 {
		return 0
	}
	return math.Float64frombits(binary.BigEndian.Uint64(b))
}

func (s *store) close() error {
	close(s.stop)
	s.flush()
	return s.db.Close()
}

func (s *store) updatePeerSeq(nodeID string, seq uint64) {
	key := "meta:seq:" + nodeID
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if seq > shard.peerSeqs[nodeID] {
		shard.peerSeqs[nodeID] = seq
		shard.dirty[key] = true
	}
}

func (s *store) getPeerSeq(nodeID string) uint64 {
	key := "meta:seq:" + nodeID
	shard := s.getShard(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	return shard.peerSeqs[nodeID]
}
