package capacitor_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/cuprite-io/capacitor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func GenerateSymmetricTLSConfig() (*tls.Config, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "capacitor-peer",
		},
		NotBefore:             time.Now().Add(-1 * time.Minute),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}

	cert, err := x509.ParseCertificate(derBytes)
	if err != nil {
		return nil, err
	}

	tlsCert := tls.Certificate{
		Certificate: [][]byte{derBytes},
		PrivateKey:  key,
	}

	certPool := x509.NewCertPool()
	certPool.AddCert(cert)

	return &tls.Config{
		Certificates:       []tls.Certificate{tlsCert},
		ClientAuth:         tls.RequireAndVerifyClientCert,
		ClientCAs:          certPool,
		RootCAs:            certPool,
		InsecureSkipVerify: true, // Required for 127.0.0.1 testing unless we handle SANs perfectly
		MinVersion:         tls.VersionTLS13,
	}, nil
}

func TestCapacitor_SecureReplication(t *testing.T) {
	tlsConfig, err := GenerateSymmetricTLSConfig()
	require.NoError(t, err)

	cfg1 := capacitor.Config{
		NodeID:     "node-1",
		StreamPort: 0,
		TLSConfig:  tlsConfig,
	}
	n1, err := capacitor.New(cfg1)
	require.NoError(t, err)
	defer n1.Close()

	cfg2 := capacitor.Config{
		NodeID:     "node-2",
		StreamPort: 0,
		Peers:      []string{fmt.Sprintf("127.0.0.1:%d", n1.Memberlist().LocalNode().Port)},
		TLSConfig:  tlsConfig,
	}
	n2, err := capacitor.New(cfg2)
	require.NoError(t, err)
	defer n2.Close()

	// Wait for connection and handshake
	time.Sleep(1 * time.Second)

	// Set on Node 1
	ctx := context.Background()
	err = n1.Set(ctx, "secure-key", "secret-value", 0)
	assert.NoError(t, err)

	// Wait for replication
	assert.Eventually(t, func() bool {
		val, _ := n2.Get(ctx, "secure-key")
		return val == "secret-value"
	}, 5*time.Second, 100*time.Millisecond)
}

func TestCapacitor_AuthTokenValidation(t *testing.T) {
	ctx := context.Background()
	token := "super-secret-password"

	// 1. Setup Node 1 with AuthToken
	cfg1 := capacitor.Config{
		NodeID:     "node-1",
		StreamPort: 0,
		AuthToken:  token,
	}
	n1, err := capacitor.New(cfg1)
	require.NoError(t, err)
	defer n1.Close()

	// 2. Setup Node 2 with WRONG AuthToken
	cfg2 := capacitor.Config{
		NodeID:     "node-2",
		StreamPort: 0,
		Peers:      []string{fmt.Sprintf("127.0.0.1:%d", n1.Memberlist().LocalNode().Port)},
		AuthToken:  "wrong-token",
	}
	n2, err := capacitor.New(cfg2)
	require.NoError(t, err)
	defer n2.Close()

	// 3. Setup Node 3 with CORRECT AuthToken
	cfg3 := capacitor.Config{
		NodeID:     "node-3",
		StreamPort: 0,
		Peers:      []string{fmt.Sprintf("127.0.0.1:%d", n1.Memberlist().LocalNode().Port)},
		AuthToken:  token,
	}
	n3, err := capacitor.New(cfg3)
	require.NoError(t, err)
	defer n3.Close()

	time.Sleep(2 * time.Second)

	// Set on Node 1
	err = n1.Set(ctx, "auth-key", "auth-value", 0)
	assert.NoError(t, err)

	// Node 3 (Correct Token) should receive it
	assert.Eventually(t, func() bool {
		val, _ := n3.Get(ctx, "auth-key")
		return val == "auth-value"
	}, 5*time.Second, 100*time.Millisecond)

	// Node 2 (Wrong Token) should NOT receive it
	val, _ := n2.Get(ctx, "auth-key")
	assert.Empty(t, val)
}
