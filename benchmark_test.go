package capacitor_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/cuprite-io/capacitor"
)

func BenchmarkCapacitor_Set(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "capacitor-bench-set-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := capacitor.Config{
		NodeID:   "bench-node",
		DataPath: tmpDir,
		BindPort: 0,
	}

	cp, err := capacitor.New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer cp.Close()

	ctx := context.Background()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cp.Set(ctx, "key", "value", 0)
	}
}

func BenchmarkCapacitor_Get(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "capacitor-bench-get-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := capacitor.Config{
		NodeID:   "bench-node",
		DataPath: tmpDir,
		BindPort: 0,
	}

	cp, err := capacitor.New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer cp.Close()

	ctx := context.Background()
	cp.Set(ctx, "key", "value", 0)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cp.Get(ctx, "key")
	}
}

func BenchmarkCapacitor_GetScan(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "capacitor-bench-getscan-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := capacitor.Config{
		NodeID:   "bench-node",
		DataPath: tmpDir,
		BindPort: 0,
	}

	cp, err := capacitor.New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer cp.Close()

	ctx := context.Background()
	cp.Set(ctx, "key", "value", 0)

	var dest string
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cp.GetScan(ctx, "key", &dest)
	}
}

type benchSession struct {
	UserID   string `msgpack:"user_id"`
	IsActive bool   `msgpack:"is_active"`
}

func BenchmarkCapacitor_GetScanStruct(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "capacitor-bench-getscanstruct-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := capacitor.Config{
		NodeID:   "bench-node",
		DataPath: tmpDir,
		BindPort: 0,
	}

	cp, err := capacitor.New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer cp.Close()

	ctx := context.Background()
	session := benchSession{UserID: "user_42", IsActive: true}
	cp.Set(ctx, "key", session, 0)

	var dest benchSession
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = cp.GetScan(ctx, "key", &dest)
	}
}

func BenchmarkCapacitor_Increment(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "capacitor-bench-incr-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := capacitor.Config{
		NodeID:   "bench-node",
		DataPath: tmpDir,
		BindPort: 0,
	}

	cp, err := capacitor.New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer cp.Close()

	ctx := context.Background()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cp.Increment(ctx, "counter")
	}
}

func BenchmarkConcurrency_Sharded(b *testing.B) {
	ctx := context.Background()
	cfg := capacitor.Config{NodeID: "bench-node", DataPath: ""}
	cp, err := capacitor.New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer cp.Close()

	// High parallelism to stress the mutexes
	b.SetParallelism(128)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i%1000) // Contention on same keys
			if i%10 == 0 {
				cp.GetCount(ctx, key)
			} else {
				cp.Increment(ctx, key)
			}
			i++
		}
	})
}
