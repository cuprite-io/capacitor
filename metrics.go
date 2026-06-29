package capacitor

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type Stat struct {
	Count int64
	Sum   int64 // Nanoseconds
	Max   int64 // Nanoseconds
}

func (s *Stat) Record(duration time.Duration) {
	d := duration.Nanoseconds()
	atomic.AddInt64(&s.Count, 1)
	atomic.AddInt64(&s.Sum, d)
	for {
		oldMax := atomic.LoadInt64(&s.Max)
		if d <= oldMax || atomic.CompareAndSwapInt64(&s.Max, oldMax, d) {
			break
		}
	}
}

type MetricsTracker struct {
	mu sync.Mutex

	// Latencies
	SetLat       Stat
	GetLat       Stat
	IncrLat      Stat
	ReplicateLat Stat // End-to-end (BornAt to Apply)

	// Memory (Peak)
	PeakAlloc     uint64
	PeakHeapAlloc uint64
	PeakSys       uint64

	stop chan struct{}
}

func NewMetricsTracker() *MetricsTracker {
	m := &MetricsTracker{
		stop: make(chan struct{}),
	}
	go m.memMonitor()
	return m
}

func (m *MetricsTracker) memMonitor() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			m.mu.Lock()
			if ms.Alloc > m.PeakAlloc {
				m.PeakAlloc = ms.Alloc
			}
			if ms.HeapAlloc > m.PeakHeapAlloc {
				m.PeakHeapAlloc = ms.HeapAlloc
			}
			if ms.Sys > m.PeakSys {
				m.PeakSys = ms.Sys
			}
			m.mu.Unlock()
		case <-m.stop:
			return
		}
	}
}

func (m *MetricsTracker) Stop() {
	close(m.stop)
}

type Summary struct {
	Metric     string
	Average    time.Duration
	Peak       time.Duration
	Count      int64
	PeakMemMB  float64
	PeakHeapMB float64
}

func (m *MetricsTracker) GetSummary() []Summary {
	m.mu.Lock()
	defer m.mu.Unlock()

	summaries := []Summary{
		m.summarize("Set", m.SetLat),
		m.summarize("Get", m.GetLat),
		m.summarize("Incr", m.IncrLat),
		m.summarize("Replication", m.ReplicateLat),
	}

	// Add pseudo-entries for memory
	summaries = append(summaries, Summary{
		Metric:     "Memory (Peak)",
		PeakMemMB:  float64(m.PeakAlloc) / 1024 / 1024,
		PeakHeapMB: float64(m.PeakHeapAlloc) / 1024 / 1024,
	})

	return summaries
}

func (m *MetricsTracker) summarize(name string, s Stat) Summary {
	count := atomic.LoadInt64(&s.Count)
	if count == 0 {
		return Summary{Metric: name}
	}
	avg := atomic.LoadInt64(&s.Sum) / count
	return Summary{
		Metric:  name,
		Count:   count,
		Average: time.Duration(avg),
		Peak:    time.Duration(atomic.LoadInt64(&s.Max)),
	}
}
