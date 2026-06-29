package capacitor

import (
	"bufio"
	"context"
	"crypto/tls"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tinylib/msgp/msgp"
)

//go:generate msgp
//msgp:ignore clientConn StreamClient StreamServer

type Handshake struct {
	LastSeenSeq uint64 `msg:"ls"`
	AuthToken   string `msg:"at,omitempty"`
}

type Batch struct {
	FromNode  string     `json:"f" msg:"f"`
	Entries   [][]byte   `json:"e" msg:"e"`
	Handshake *Handshake `json:"h,omitempty" msg:"h,omitempty"`
}

var batchPool = sync.Pool{
	New: func() any {
		return make([]byte, 0, 1024*1024) // 1MB initial buffer
	},
}

// putBatchBuffer returns a buffer to the pool if it hasn't grown too large
func putBatchBuffer(buf []byte) {
	// Guardrail: Don't pool buffers larger than 16MB to prevent memory bloat
	if cap(buf) <= 16*1024*1024 {
		batchPool.Put(buf)
	}
}

type StreamServer struct {
	cp        *Capacitor
	listener  net.Listener
	tlsConfig *tls.Config
	stop      chan struct{}
}

func NewStreamServer(cp *Capacitor, addr string, tlsConfig *tls.Config) (*StreamServer, error) {
	var ln net.Listener
	var err error
	if tlsConfig != nil {
		ln, err = tls.Listen("tcp", addr, tlsConfig)
	} else {
		ln, err = net.Listen("tcp", addr)
	}

	if err != nil {
		return nil, err
	}
	return &StreamServer{
		cp:        cp,
		listener:  ln,
		tlsConfig: tlsConfig,
		stop:      make(chan struct{}),
	}, nil
}

func (s *StreamServer) Start() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.stop:
				return
			default:
				continue
			}
		}
		go s.handleConn(conn)
	}
}

func (s *StreamServer) handleConn(conn net.Conn) {
	defer conn.Close()

	if s.tlsConfig != nil {
		if tc, ok := conn.(*tls.Conn); ok {
			if err := tc.Handshake(); err != nil {
				s.cp.logger.Error("TLS Handshake error (Server)", "error", err)
				return
			}
		}
	}

	// Security: Limit the total size of data from a single connection if needed
	reader := msgp.NewReader(bufio.NewReader(io.LimitReader(conn, 100*1024*1024))) // 100MB max per connection

	firstBatch := true
	for {
		var batch Batch
		if err := batch.DecodeMsg(reader); err != nil {
			if err != io.EOF {
				s.cp.logger.Error("Stream error", "error", err)
			}
			return
		}

		// 1. Process Handshake if present (Peer is telling us where they left off in OUR log)
		if batch.Handshake != nil {
			// Security: Validate AuthToken if configured
			if s.cp.authToken != "" && batch.Handshake.AuthToken != s.cp.authToken {
				s.cp.logger.Warn("unauthorized connection attempt", "from", batch.FromNode)
				return
			}
			s.cp.peerSeqs.Store(batch.FromNode, batch.Handshake.LastSeenSeq)
		} else if firstBatch && s.cp.authToken != "" {
			// If AuthToken is required, the first batch MUST contain a handshake with it
			s.cp.logger.Warn("missing handshake auth token", "from", batch.FromNode)
			return
		}

		firstBatch = false

		// 2. Apply batch to local state
		for _, rawEntry := range batch.Entries {
			var entry LogEntry
			if _, err := entry.UnmarshalMsg(rawEntry); err != nil {
				continue
			}
			s.cp.applyRemoteEntry(context.Background(), entry)
		}
	}
}

func (s *StreamServer) Stop() {
	close(s.stop)
	s.listener.Close()
}

type clientConn struct {
	net.Conn
	lastUsed      atomic.Int64 // UnixNano
	handshakeDone bool
}

type StreamClient struct {
	mu        sync.RWMutex
	conns     map[string]*clientConn
	tlsConfig *tls.Config
	authToken string
	stop      chan struct{}
}

func NewStreamClient(tlsConfig *tls.Config, authToken string) *StreamClient {
	c := &StreamClient{
		conns:     make(map[string]*clientConn),
		tlsConfig: tlsConfig,
		authToken: authToken,
		stop:      make(chan struct{}),
	}
	go c.janitor()
	return c
}

func (c *StreamClient) janitor() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.mu.Lock()
			now := time.Now().UnixNano()
			for nodeID, cc := range c.conns {
				if now-cc.lastUsed.Load() > int64(2*time.Minute) {
					cc.Close()
					delete(c.conns, nodeID)
				}
			}
			c.mu.Unlock()
		case <-c.stop:
			return
		}
	}
}

func (c *StreamClient) dial(ctx context.Context, nodeID string, addr string) (*clientConn, error) {
	c.mu.RLock()
	cc, ok := c.conns[nodeID]
	if ok {
		cc.lastUsed.Store(time.Now().UnixNano())
	}
	c.mu.RUnlock()

	if ok {
		return cc, nil
	}

	d := &net.Dialer{
		Timeout:   2 * time.Second,
		KeepAlive: 30 * time.Second,
	}

	var newConn net.Conn
	var err error
	if c.tlsConfig != nil {
		rawConn, err := d.DialContext(ctx, "tcp", addr)
		if err != nil {
			return nil, err
		}

		tlsConn := tls.Client(rawConn, c.tlsConfig)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			rawConn.Close()
			return nil, err
		}
		newConn = tlsConn
	} else {
		newConn, err = d.DialContext(ctx, "tcp", addr)
	}

	if err != nil {
		return nil, err
	}

	cc = &clientConn{
		Conn: newConn,
	}
	cc.lastUsed.Store(time.Now().UnixNano())

	c.mu.Lock()
	if existing, already := c.conns[nodeID]; already {
		newConn.Close()
		cc = existing
	} else {
		c.conns[nodeID] = cc
	}
	c.mu.Unlock()
	return cc, nil
}

func (c *StreamClient) SendBatch(ctx context.Context, nodeID string, addr string, batch Batch, lastSeenPeerSeq uint64) error {
	cc, err := c.dial(ctx, nodeID, addr)
	if err != nil {
		return err
	}

	if !cc.handshakeDone {
		batch.Handshake = &Handshake{
			LastSeenSeq: lastSeenPeerSeq,
			AuthToken:   c.authToken,
		}
		cc.handshakeDone = true
	}

	buf := batchPool.Get().([]byte)
	data, err := batch.MarshalMsg(buf[:0])
	if err != nil {
		putBatchBuffer(buf)
		return err
	}
	defer putBatchBuffer(data)

	if deadline, ok := ctx.Deadline(); ok {
		cc.SetWriteDeadline(deadline)
		defer cc.SetWriteDeadline(time.Time{})
	}

	_, err = cc.Write(data)
	if err != nil {
		cc.Close()
		c.mu.Lock()
		if current, ok := c.conns[nodeID]; ok && current == cc {
			delete(c.conns, nodeID)
		}
		c.mu.Unlock()
		return err
	}
	return nil
}

func (c *StreamClient) CloseConn(nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cc, ok := c.conns[nodeID]; ok {
		cc.Close()
		delete(c.conns, nodeID)
	}
}

func (c *StreamClient) Close() {
	close(c.stop)
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, cc := range c.conns {
		cc.Close()
	}
	c.conns = make(map[string]*clientConn)
}
