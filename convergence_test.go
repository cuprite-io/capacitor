package capacitor_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/cuprite-io/capacitor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCapacitor_ThreeNodeFullSync(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping high-volume test in short mode")
	}

	const totalKeys = 1_000_000
	ctx := context.Background()

	// 1. Setup 3-node cluster
	nodes := make([]*capacitor.Capacitor, 3)
	dirs := make([]string, 3)
	for i := 0; i < 3; i++ {
		var err error
		dirs[i], err = os.MkdirTemp("", fmt.Sprintf("capacitor-convergence-%d-*", i))
		require.NoError(t, err)
		cfg := capacitor.Config{
			NodeID:     fmt.Sprintf("node-%d", i),
			DataPath:   dirs[i],
			BindPort:   0,
			StreamPort: 0,
		}
		if i > 0 {
			cfg.Peers = []string{nodes[0].Memberlist().LocalNode().Addr.String() + ":" + fmt.Sprintf("%d", nodes[0].Memberlist().LocalNode().Port)}
		}
		nodes[i], err = capacitor.New(cfg)
		require.NoError(t, err)
	}
	defer func() {
		for i := 0; i < 3; i++ {
			nodes[i].Close()
			os.RemoveAll(dirs[i])
		}
	}()

	time.Sleep(2 * time.Second) // Let cluster settle

	// 2. Node 0 writes 1M keys
	fmt.Printf("Node 0: Writing %d keys...\n", totalKeys)
	for i := 0; i < totalKeys; i++ {
		nodes[0].Set(ctx, fmt.Sprintf("k-%d", i), fmt.Sprintf("v-%d", i), 0)
	}

	// 3. Wait for synchronization
	fmt.Println("Waiting for Nodes 1 and 2 to sync...")
	start := time.Now()
	for {
		c1 := nodes[1].Store().CacheSize()
		c2 := nodes[2].Store().CacheSize()

		if c1 == totalKeys && c2 == totalKeys {
			break
		}
		if time.Since(start) > 30*time.Second {
			t.Fatalf("Sync timed out. Node 1: %d, Node 2: %d", c1, c2)
		}
		time.Sleep(500 * time.Millisecond)
	}
	fmt.Printf("Sync complete in %v\n", time.Since(start))

	// 4. Verify half on Node 1, half on Node 2
	fmt.Println("Verifying data integrity...")
	for i := 0; i < totalKeys; i++ {
		key := fmt.Sprintf("k-%d", i)
		expected := fmt.Sprintf("v-%d", i)

		var val string
		var err error
		if i%2 == 0 {
			val, err = nodes[1].Get(ctx, key)
		} else {
			val, err = nodes[2].Get(ctx, key)
		}

		if err != nil || val != expected {
			t.Fatalf("Data mismatch at key %s. Got %v, want %v", key, val, expected)
		}
	}
	fmt.Println("Verification Success: 1,000,000 keys verified across remote nodes.")
}

func TestCapacitor_ComplexConvergence(t *testing.T) {
	// This test simulates a "Conflict Storm"
	// 2 nodes updating the same keys and counters simultaneously.
	// We verify that they eventually settle on the exact same state.

	const opsPerNode = 50_000
	ctx := context.Background()

	// 1. Setup 2 nodes
	n1Dir, err := os.MkdirTemp("", "n1")
	require.NoError(t, err)
	n2Dir, err := os.MkdirTemp("", "n2")
	require.NoError(t, err)
	defer os.RemoveAll(n1Dir)
	defer os.RemoveAll(n2Dir)

	n1, err := capacitor.New(capacitor.Config{NodeID: "n1", DataPath: n1Dir})
	require.NoError(t, err)
	n2, err := capacitor.New(capacitor.Config{NodeID: "n2", DataPath: n2Dir, Peers: []string{n1.Memberlist().LocalNode().Addr.String() + ":" + fmt.Sprintf("%d", n1.Memberlist().LocalNode().Port)}})
	require.NoError(t, err)
	defer n1.Close()
	defer n2.Close()

	time.Sleep(1 * time.Second)

	var wg sync.WaitGroup
	wg.Add(2)

	// Node 1 work
	go func() {
		defer wg.Done()
		for i := 0; i < opsPerNode; i++ {
			n1.Increment(ctx, "shared-counter")
			n1.Set(ctx, "last-writer-key", "val-from-n1", 0)
		}
	}()

	// Node 2 work
	go func() {
		defer wg.Done()
		for i := 0; i < opsPerNode; i++ {
			n2.Increment(ctx, "shared-counter")
			// Slightly delay N2 so it likely wins LWW
			time.Sleep(1 * time.Microsecond)
			n2.Set(ctx, "last-writer-key", "val-from-n2", 0)
		}
	}()

	wg.Wait()
	fmt.Println("Conflict storm finished. Waiting for convergence...")
	time.Sleep(5 * time.Second)

	// Verify Counter Convergence (CRDT)
	c1, _ := n1.GetCount(ctx, "shared-counter")
	c2, _ := n2.GetCount(ctx, "shared-counter")
	assert.Equal(t, int64(opsPerNode*2), c1, "Node 1 counter should be total sum")
	assert.Equal(t, int64(opsPerNode*2), c2, "Node 2 counter should be total sum")

	// Verify LWW Convergence (HLC)
	v1, _ := n1.Get(ctx, "last-writer-key")
	v2, _ := n2.Get(ctx, "last-writer-key")
	assert.Equal(t, v1, v2, "Nodes must agree on the same value for LWW key")
	fmt.Printf("Final Counter: %d, Final Winner: %s\n", c1, v1)
}
