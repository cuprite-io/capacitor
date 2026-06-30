package capacitor

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var ErrClockSmash = errors.New("HLC clock smash detected")

const DefaultMaxOffset = 500 * time.Millisecond

// HLC (Hybrid Logical Clock) implements a causality-preserving clock.
type HLC struct {
	mu        sync.Mutex
	physical  int64
	logical   int32
	maxOffset int64 // Maximum allowed drift from local wall clock
}

//go:generate msgp

type Timestamp struct {
	Physical int64 `json:"p" msg:"p"`
	Logical  int32 `json:"l" msg:"l"`
}

// GreaterOrEqual returns true if this timestamp is logically greater than or equal to the other timestamp.
func (t Timestamp) GreaterOrEqual(other Timestamp) bool {
	if t.Physical != other.Physical {
		return t.Physical > other.Physical
	}
	return t.Logical >= other.Logical
}

func NewHLC() *HLC {
	return &HLC{
		physical:  time.Now().UnixNano(),
		maxOffset: int64(DefaultMaxOffset),
	}
}

// SetMaxOffset sets the maximum allowed clock drift.
func (h *HLC) SetMaxOffset(offset time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.maxOffset = int64(offset)
}

// Now generates a new timestamp.
func (h *HLC) Now() Timestamp {
	h.mu.Lock()
	defer h.mu.Unlock()

	wall := time.Now().UnixNano()
	if h.physical < wall {
		h.physical = wall
		h.logical = 0
	} else {
		h.logical++
	}

	return Timestamp{
		Physical: h.physical,
		Logical:  h.logical,
	}
}

// Update updates the local clock based on a received remote timestamp.
func (h *HLC) Update(remote Timestamp) (Timestamp, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	wall := time.Now().UnixNano()

	// 1. Clock Smash Protection: Reject any timestamp that exceeds wall + maxOffset
	if remote.Physical > wall+h.maxOffset {
		return Timestamp{}, ErrClockSmash
	}

	// Choose the max physical time
	maxPhysical := wall
	if h.physical > maxPhysical {
		maxPhysical = h.physical
	}
	if remote.Physical > maxPhysical {
		maxPhysical = remote.Physical
	}

	if maxPhysical == h.physical && maxPhysical == remote.Physical {
		// All physical times are same, choose max logical + 1
		maxLogical := h.logical
		if remote.Logical > maxLogical {
			maxLogical = remote.Logical
		}
		h.logical = maxLogical + 1
	} else if maxPhysical == h.physical {
		h.logical++
	} else if maxPhysical == remote.Physical {
		h.physical = maxPhysical
		h.logical = remote.Logical + 1
	} else {
		h.physical = maxPhysical
		h.logical = 0
	}

	return Timestamp{
		Physical: h.physical,
		Logical:  h.logical,
	}, nil
}

func (t Timestamp) After(other Timestamp) bool {
	if t.Physical > other.Physical {
		return true
	}
	if t.Physical < other.Physical {
		return false
	}
	return t.Logical > other.Logical
}

func (t Timestamp) String() string {
	return fmt.Sprintf("%d:%d", t.Physical, t.Logical)
}
