package capacitor

import (
	"sync"
)

//go:generate msgp

// LogEntry represents a single operation in the delta log.
type LogEntry struct {
	Seq    uint64    `json:"s" msg:"s"`
	TS     Timestamp `json:"t" msg:"t"`
	BornAt int64     `json:"ba" msg:"ba"` // NEW: For end-to-end latency tracking
	Op     MsgType   `json:"o" msg:"o"`
	Key    string    `json:"k" msg:"k"`
	NodeID string    `json:"n,omitempty" msg:"n,omitempty"`
	Value  []byte    `json:"v,omitempty" msg:"v,omitempty"`
	Delta  float64   `json:"d,omitempty" msg:"d,omitempty"`
	TTL    int64     `json:"ttl,omitempty" msg:"ttl,omitempty"`
}

type entryInfo struct {
	offset uint32
	length uint32
}

// DeltaLog is an in-memory binary circular buffer of operations.
type DeltaLog struct {
	mu       sync.RWMutex
	cond     *sync.Cond
	index    []entryInfo // Ring buffer for metadata
	data     []byte      // Pre-allocated binary data
	head     uint64      // Next sequence ID
	bufHead  uint32      // Current write offset in data
	capacity uint64      // Max number of entries in index
	bufCap   uint32      // Capacity of data buffer
}

func NewDeltaLog(capacity uint64) *DeltaLog {
	// Default buffer size: 256MB for 1M entries (avg 256 bytes per entry)
	bufSize := uint32(256 * 1024 * 1024)
	if capacity < 100000 {
		bufSize = uint32(capacity * 256)
	}

	l := &DeltaLog{
		index:    make([]entryInfo, capacity),
		data:     make([]byte, bufSize),
		capacity: capacity,
		bufCap:   bufSize,
		head:     1,
	}
	l.cond = sync.NewCond(&l.mu)
	return l
}

func (l *DeltaLog) Append(entry LogEntry) uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	entry.Seq = l.head

	// Pre-calculate size to avoid re-allocations
	sz := entry.Msgsize()

	// Check for wrap-around in data buffer
	if l.bufHead+uint32(sz) > l.bufCap {
		l.bufHead = 0
	}

	// Marshal directly into the pre-allocated buffer
	// We use a sub-slice with 0 length but capacity to the end
	start := l.bufHead
	out, _ := entry.MarshalMsg(l.data[start:start:l.bufCap])
	actualLen := uint32(len(out))

	l.index[l.head%l.capacity] = entryInfo{
		offset: start,
		length: actualLen,
	}

	l.bufHead += actualLen
	l.head++

	l.cond.Broadcast()
	return entry.Seq
}

func (l *DeltaLog) GetEntriesRaw(startSeq uint64, limit int, maxBytes int) [][]byte {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if startSeq >= l.head {
		return nil
	}

	// Check if startSeq is too old (overwritten)
	earliest := uint64(1)
	if l.head > l.capacity {
		earliest = l.head - l.capacity
	}
	if startSeq < earliest {
		startSeq = earliest
	}

	count := int(l.head - startSeq)
	if count > limit {
		count = limit
	}

	res := make([][]byte, 0, count)
	currentBytes := 0
	for i := 0; i < count; i++ {
		info := l.index[(startSeq+uint64(i))%l.capacity]

		// If adding this entry exceeds maxBytes, stop here (unless it's the first entry)
		if len(res) > 0 && currentBytes+int(info.length) > maxBytes {
			break
		}

		// We MUST copy the bytes here because the circular buffer can wrap around
		// while the network write is in progress outside this lock.
		entryCopy := make([]byte, info.length)
		copy(entryCopy, l.data[info.offset:info.offset+info.length])

		res = append(res, entryCopy)
		currentBytes += int(info.length)
	}
	return res
}

func (l *DeltaLog) Head() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.head - 1
}
