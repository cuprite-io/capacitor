package capacitor

import (
	"github.com/hashicorp/memberlist"
)

// Exported fields/methods for testing from external package (capacitor_test).

func (cp *Capacitor) Memberlist() *memberlist.Memberlist {
	return cp.ml
}

func (cp *Capacitor) Store() *store {
	return cp.store
}

func (cp *Capacitor) StreamAddr() string {
	return cp.streamAddr
}

func (cp *Capacitor) StartPeerReplicator(name, addr string) {
	cp.startPeerReplicator(name, addr)
}

func (h *HLC) SetPhysicalTimeForTest(physical int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.physical = physical
}

func (h *HLC) GetInternalTimeForTest() (int64, int32) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.physical, h.logical
}

func (s *store) GetWindowSizeForTest(key string) int {
	shard := s.getShard(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	return len(shard.windows[key])
}

func (s *store) SetWithTSTest(key string, value any, ts Timestamp) error {
	return s.set(key, value, ts, 0)
}
