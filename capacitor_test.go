package capacitor_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/cuprite-io/capacitor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCapacitor_Local(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "capacitor-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cfg := capacitor.Config{
		NodeID:   "node1",
		DataPath: tmpDir,
		BindPort: 0, // Random port
	}

	cp, err := capacitor.New(cfg)
	require.NoError(t, err)
	defer cp.Close()

	ctx := context.Background()

	t.Run("Set and Get", func(t *testing.T) {
		err := cp.Set(ctx, "key1", "value1", 0)
		assert.NoError(t, err)

		val, err := cp.Get(ctx, "key1")
		assert.NoError(t, err)
		assert.Equal(t, "value1", val)
	})

	t.Run("Increment", func(t *testing.T) {
		count, err := cp.Increment(ctx, "counter1")
		assert.NoError(t, err)
		assert.Equal(t, int64(1), count)

		count, err = cp.IncrementBy(ctx, "counter1", 5)
		assert.NoError(t, err)
		assert.Equal(t, int64(6), count)
	})

	t.Run("Metric", func(t *testing.T) {
		m, err := cp.IncrementMetric(ctx, "metric1", 10.5)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), m.Count)
		assert.Equal(t, 10.5, m.Sum)
	})
}

func TestCapacitor_Cluster(t *testing.T) {
	// Setup 3 nodes
	nodes := make([]*capacitor.Capacitor, 3)
	dirs := make([]string, 3)

	for i := 0; i < 3; i++ {
		tmpDir, err := os.MkdirTemp("", fmt.Sprintf("capacitor-cluster-%d-*", i))
		require.NoError(t, err)
		dirs[i] = tmpDir

		cfg := capacitor.Config{
			NodeID:   fmt.Sprintf("node-%d", i),
			DataPath: tmpDir,
			BindPort: 0,
		}
		if i > 0 {
			// Join previous node
			cfg.Peers = []string{fmt.Sprintf("127.0.0.1:%d", nodes[0].Memberlist().LocalNode().Port)}
		}

		cp, err := capacitor.New(cfg)
		require.NoError(t, err)
		nodes[i] = cp
	}

	defer func() {
		for i := 0; i < 3; i++ {
			nodes[i].Close()
			os.RemoveAll(dirs[i])
		}
	}()

	// Wait for cluster to settle
	assert.Eventually(t, func() bool {
		return nodes[0].Memberlist().NumMembers() == 3
	}, 5*time.Second, 100*time.Millisecond)

	ctx := context.Background()

	t.Run("Distributed Increment", func(t *testing.T) {
		// Each node increments the same key
		_, err0 := nodes[0].Increment(ctx, "dist-count")
		require.NoError(t, err0)
		_, err1 := nodes[1].Increment(ctx, "dist-count")
		require.NoError(t, err1)
		_, err2 := nodes[2].Increment(ctx, "dist-count")
		require.NoError(t, err2)

		// Wait for gossip and verify
		assert.Eventually(t, func() bool {
			for i := 0; i < 3; i++ {
				count, err := nodes[i].GetCount(ctx, "dist-count")
				if err != nil || count != 3 {
					return false
				}
			}
			return true
		}, 5*time.Second, 100*time.Millisecond)
	})

	t.Run("LWW Conflict Resolution", func(t *testing.T) {
		// Node 0 sets value
		err := nodes[0].Set(ctx, "lww-key", "val-old", 0)
		require.NoError(t, err)

		// Node 1 sets newer value (HLC handles ordering, even if fast)
		time.Sleep(50 * time.Millisecond)
		err = nodes[1].Set(ctx, "lww-key", "val-new", 0)
		require.NoError(t, err)

		// Wait for replication and verify
		assert.Eventually(t, func() bool {
			for i := 0; i < 3; i++ {
				val, err := nodes[i].Get(ctx, "lww-key")
				if err != nil || val != "val-new" {
					return false
				}
			}
			return true
		}, 5*time.Second, 100*time.Millisecond)
	})

	t.Run("Distributed Sliding Window", func(t *testing.T) {
		// Node 0 increments sliding window
		_, err := nodes[0].IncrementSlidingWindow(ctx, "dist-window", 1*time.Minute)
		require.NoError(t, err)

		// Wait for replication and verify
		assert.Eventually(t, func() bool {
			for i := 0; i < 3; i++ {
				if nodes[i].Store().GetWindowSizeForTest("dist-window") != 1 {
					return false
				}
			}
			return true
		}, 5*time.Second, 100*time.Millisecond)
	})
}

func TestHLC_ClockSmashProtection(t *testing.T) {
	hlc := capacitor.NewHLC()
	hlc.SetMaxOffset(500 * time.Millisecond)

	// Ensure local HLC is way in the past
	hlc.SetPhysicalTimeForTest(100) // Unix Nano 100

	// 1. Normal Update (Remote is way in the future relative to 100, but relative to REAL NOW it is sane)
	realNow := time.Now().UnixNano()

	// Case: Remote is 100ms ahead of real wall clock
	remoteSane := capacitor.Timestamp{
		Physical: realNow + int64(100*time.Millisecond),
		Logical:  0,
	}
	resSane, err := hlc.Update(remoteSane)
	assert.NoError(t, err, "Should adopt remote time within threshold")
	assert.Equal(t, remoteSane.Physical, resSane.Physical)

	// 2. Clock Smash Update (Remote is Year 2099)
	futureTime := time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	remoteSmashed := capacitor.Timestamp{
		Physical: futureTime,
		Logical:  0,
	}

	// Save state before smashed update
	preSmashedPhysical, preSmashedLogical := hlc.GetInternalTimeForTest()

	_, err = hlc.Update(remoteSmashed)
	assert.Error(t, err, "Should return error for smashed timestamp")
	assert.Equal(t, capacitor.ErrClockSmash, err)

	// Verify local HLC was NOT updated
	postPhysical, postLogical := hlc.GetInternalTimeForTest()
	assert.Equal(t, preSmashedPhysical, postPhysical, "Local physical clock should not change on error")
	assert.Equal(t, preSmashedLogical, postLogical, "Local logical clock should not change on error")
}

func TestCapacitor_LogicalClockConflictResolution(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "capacitor-hlc-logical-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cp, err := capacitor.New(capacitor.Config{NodeID: "node1", DataPath: tmpDir, BindPort: 0})
	require.NoError(t, err)
	defer cp.Close()

	ctx := context.Background()

	// Setup two timestamps at the identical physical time but different logical ticks
	t0 := capacitor.Timestamp{Physical: 1000, Logical: 0}
	t1 := capacitor.Timestamp{Physical: 1000, Logical: 1}

	// 1. Apply t1 (newer logical component)
	err = cp.Store().SetWithTSTest("key", "value-new", t1)
	require.NoError(t, err)

	// 2. Try to apply t0 (older logical component, same physical time)
	err = cp.Store().SetWithTSTest("key", "value-old", t0)
	require.NoError(t, err)

	// 3. Verify that the newer logical write won (LWW) and old value was rejected
	val, err := cp.Get(ctx, "key")
	require.NoError(t, err)
	assert.Equal(t, "value-new", val)
}

func TestCapacitor_TTLReplicationAndEviction(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tmpDir1, err := os.MkdirTemp("", "capacitor-ttl-node1-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir1)

	tmpDir2, err := os.MkdirTemp("", "capacitor-ttl-node2-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir2)

	cfg1 := capacitor.Config{
		NodeID:     "node1",
		BindPort:   18001,
		StreamPort: 18002,
		DataPath:   tmpDir1,
	}
	cfg2 := capacitor.Config{
		NodeID:     "node2",
		BindPort:   18003,
		StreamPort: 18004,
		DataPath:   tmpDir2,
		Peers:      []string{"127.0.0.1:18001"},
	}

	n1, err := capacitor.New(cfg1)
	require.NoError(t, err)
	defer n1.Close()

	n2, err := capacitor.New(cfg2)
	require.NoError(t, err)
	defer n2.Close()

	// Wait for nodes to discover each other
	assert.Eventually(t, func() bool {
		return n1.Memberlist().NumMembers() == 2 && n2.Memberlist().NumMembers() == 2
	}, 5*time.Second, 100*time.Millisecond)

	// Set key on Node 1 with a short TTL of 300 milliseconds
	ttl := 300 * time.Millisecond
	err = n1.Set(ctx, "ttl-key", "ttl-val", ttl)
	require.NoError(t, err)

	// 1. Verify it exists immediately on Node 1
	val1, err := n1.Get(ctx, "ttl-key")
	require.NoError(t, err)
	assert.Equal(t, "ttl-val", val1)

	// 2. Verify it gets replicated to Node 2 and is accessible before it expires
	assert.Eventually(t, func() bool {
		val2, err := n2.Get(ctx, "ttl-key")
		return err == nil && val2 == "ttl-val"
	}, 1*time.Second, 50*time.Millisecond)

	// 3. Wait for TTL to expire (300ms + some buffer) and check both nodes have evicted the key
	time.Sleep(500 * time.Millisecond)

	// Verify key is gone
	val1, err = n1.Get(ctx, "ttl-key")
	require.NoError(t, err)
	assert.Empty(t, val1, "Key should be evicted on Node 1 after TTL expiration")

	val2, err := n2.Get(ctx, "ttl-key")
	require.NoError(t, err)
	assert.Empty(t, val2, "Key should be evicted on Node 2 after TTL expiration")
}


