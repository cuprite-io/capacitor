package capacitor_test

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/cuprite-io/capacitor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// chaosProxy is a simple TCP proxy that can inject latency and drop connections.
type chaosProxy struct {
	mu          sync.Mutex
	listener    net.Listener
	targetAddr  string
	latency     time.Duration
	dropChance  float64 // 0.0 to 1.0
	activeConns map[net.Conn]struct{}
	stop        chan struct{}
}

func newChaosProxy(targetAddr string) (*chaosProxy, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	return &chaosProxy{
		listener:    ln,
		targetAddr:  targetAddr,
		activeConns: make(map[net.Conn]struct{}),
		stop:        make(chan struct{}),
	}, nil
}

func (p *chaosProxy) Addr() string {
	return p.listener.Addr().String()
}

func (p *chaosProxy) SetChaos(latency time.Duration, dropChance float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.latency = latency
	p.dropChance = dropChance
}

func (p *chaosProxy) Start() {
	for {
		conn, err := p.listener.Accept()
		if err != nil {
			select {
			case <-p.stop:
				return
			default:
				continue
			}
		}
		go p.handleConn(conn)
	}
}

func (p *chaosProxy) handleConn(in net.Conn) {
	p.mu.Lock()
	p.activeConns[in] = struct{}{}
	p.mu.Unlock()

	defer func() {
		in.Close()
		p.mu.Lock()
		delete(p.activeConns, in)
		p.mu.Unlock()
	}()

	out, err := net.Dial("tcp", p.targetAddr)
	if err != nil {
		return
	}
	defer out.Close()

	errChan := make(chan error, 2)

	copyLoop := func(dst, src net.Conn) {
		buf := make([]byte, 32*1024)
		for {
			n, err := src.Read(buf)
			if n > 0 {
				p.mu.Lock()
				lat := p.latency
				p.mu.Unlock()

				if lat > 0 {
					time.Sleep(lat)
				}

				_, werr := dst.Write(buf[:n])
				if werr != nil {
					errChan <- werr
					return
				}
			}
			if err != nil {
				errChan <- err
				return
			}
		}
	}

	go copyLoop(out, in)
	go copyLoop(in, out)

	<-errChan
}

func (p *chaosProxy) Stop() {
	close(p.stop)
	p.listener.Close()
	p.mu.Lock()
	for c := range p.activeConns {
		c.Close()
	}
	p.mu.Unlock()
}

func TestCapacitor_ChaosNetwork(t *testing.T) {
	ctx := context.Background()

	// 1. Setup Node 1 (Target)
	cfg1 := capacitor.Config{
		NodeID:     "node-1",
		StreamPort: 0,
	}
	n1, err := capacitor.New(cfg1)
	require.NoError(t, err)
	defer n1.Close()

	// 2. Setup Chaos Proxy for Node 1
	proxy, err := newChaosProxy(n1.StreamAddr())
	require.NoError(t, err)
	go proxy.Start()
	defer proxy.Stop()

	// 3. Setup Node 2 (Replicator)
	cfg2 := capacitor.Config{
		NodeID:     "node-2",
		StreamPort: 0,
		Peers:      []string{fmt.Sprintf("127.0.0.1:%d", n1.Memberlist().LocalNode().Port)},
	}
	n2, err := capacitor.New(cfg2)
	require.NoError(t, err)
	defer n2.Close()

	n2.StartPeerReplicator("node-1", proxy.Addr())

	// Test Case 1: High Latency (300ms)
	proxy.SetChaos(300*time.Millisecond, 0)

	start := time.Now()
	err = n2.Set(ctx, "chaos-lat-key", "slow-value", 0)
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		val, _ := n1.Get(ctx, "chaos-lat-key")
		return val == "slow-value"
	}, 10*time.Second, 100*time.Millisecond)

	t.Logf("Replication with 300ms latency took %v", time.Since(start))

	// Test Case 2: Intermittent Connection Drops
	go func() {
		for i := 0; i < 5; i++ {
			time.Sleep(1 * time.Second)
			proxy.mu.Lock()
			for c := range proxy.activeConns {
				c.Close()
			}
			proxy.mu.Unlock()
		}
	}()

	for i := 0; i < 100; i++ {
		n2.Set(ctx, fmt.Sprintf("chaos-drop-%d", i), "resilient", 0)
		time.Sleep(50 * time.Millisecond)
	}

	// Verify eventual consistency
	assert.Eventually(t, func() bool {
		count := 0
		for i := 0; i < 100; i++ {
			val, _ := n1.Get(ctx, fmt.Sprintf("chaos-drop-%d", i))
			if val == "resilient" {
				count++
			}
		}
		return count == 100
	}, 20*time.Second, 500*time.Millisecond)
}
