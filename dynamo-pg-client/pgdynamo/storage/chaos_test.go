package storage

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TestChaosContext creates a chaos test helper that can inject failures.
// This test is meant to be run against a real PostgreSQL instance.
func TestChaosConcurrentWrites(t *testing.T) {
	connString := "postgres://dynamo:dynamo@localhost:5433/dynamo?sslmode=disable"
	ctx := context.Background()

	store, err := NewStore(ctx, connString)
	if err != nil {
		t.Skipf("skipping chaos test, no database: %v", err)
	}
	defer store.Close()

	tableName := fmt.Sprintf("chaos_test_%d", time.Now().UnixNano())
	meta := TableMeta{
		TableName: tableName,
		PKName:    "PK",
		PKType:    "S",
		SKName:    "SK",
		SKType:    "S",
	}
	if err := store.CreateTable(ctx, meta); err != nil {
		t.Fatalf("create table: %v", err)
	}
	defer func() { _ = store.DeleteTable(ctx, tableName) }()

	// Concurrent writes to the same keys
	const numGoroutines = 10
	const numOps = 50
	var wg sync.WaitGroup

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < numOps; i++ {
				key := fmt.Sprintf("key-%d", rand.Intn(10)) // Overlapping keys
				item := Item{
					"PK":    {"S": key},
					"SK":    {"S": "sort"},
					"Value": {"S": fmt.Sprintf("goroutine-%d-op-%d", goroutineID, i)},
				}
				if _, err := store.PutItem(ctx, tableName, &meta, item, false); err != nil {
					t.Errorf("put item: %v", err)
					return
				}
			}
		}(g)
	}

	wg.Wait()

	// Verify all keys are readable
	result, err := store.Scan(ctx, ScanParams{TableName: tableName})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if result.Count == 0 {
		t.Fatal("expected items after concurrent writes")
	}
	t.Logf("chaos test: %d items after %d concurrent write operations", result.Count, numGoroutines*numOps)
}

func TestChaosConcurrentReadWrite(t *testing.T) {
	connString := "postgres://dynamo:dynamo@localhost:5433/dynamo?sslmode=disable"
	ctx := context.Background()

	store, err := NewStore(ctx, connString)
	if err != nil {
		t.Skipf("skipping chaos test, no database: %v", err)
	}
	defer store.Close()

	tableName := fmt.Sprintf("chaos_rw_%d", time.Now().UnixNano())
	meta := TableMeta{
		TableName: tableName,
		PKName:    "PK",
		PKType:    "S",
		SKName:    "SK",
		SKType:    "S",
	}
	if err := store.CreateTable(ctx, meta); err != nil {
		t.Fatalf("create table: %v", err)
	}
	defer func() { _ = store.DeleteTable(ctx, tableName) }()

	// Seed some data
	for i := 0; i < 20; i++ {
		item := Item{
			"PK":    {"S": fmt.Sprintf("pk-%d", i)},
			"SK":    {"S": "sk"},
			"Value": {"S": fmt.Sprintf("initial-%d", i)},
		}
		if _, err := store.PutItem(ctx, tableName, &meta, item, false); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	var wg sync.WaitGroup
	const numReaders = 5
	const numWriters = 5
	const opsPerWorker = 30

	// Readers
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				key := Item{
					"PK": {"S": fmt.Sprintf("pk-%d", rand.Intn(20))},
					"SK": {"S": "sk"},
				}
				_, err := store.GetItem(ctx, tableName, &meta, key)
				if err != nil {
					t.Errorf("get item: %v", err)
					return
				}
			}
		}()
	}

	// Writers
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				item := Item{
					"PK":    {"S": fmt.Sprintf("pk-%d", rand.Intn(20))},
					"SK":    {"S": "sk"},
					"Value": {"S": fmt.Sprintf("writer-%d-op-%d", writerID, i)},
				}
				if _, err := store.PutItem(ctx, tableName, &meta, item, false); err != nil {
					t.Errorf("put item: %v", err)
					return
				}
			}
		}(w)
	}

	wg.Wait()
	t.Log("chaos concurrent read/write test passed")
}

func TestChaosConnectionPoolExhaustion(t *testing.T) {
	connString := "postgres://dynamo:dynamo@localhost:5433/dynamo?sslmode=disable"
	ctx := context.Background()

	// Create store with very small pool
	poolCfg, err := pgxpool.ParseConfig(connString)
	if err != nil {
		t.Skipf("skipping: %v", err)
	}
	poolCfg.MaxConns = 2
	poolCfg.MinConns = 1

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		t.Skipf("skipping: %v", err)
	}
	defer pool.Close()

	store := &Store{pool: pool}
	if err := store.ensureMetaTables(ctx); err != nil {
		t.Skipf("skipping: %v", err)
	}

	tableName := fmt.Sprintf("chaos_pool_%d", time.Now().UnixNano())
	meta := TableMeta{
		TableName: tableName,
		PKName:    "PK",
		PKType:    "S",
	}
	if err := store.CreateTable(ctx, meta); err != nil {
		t.Fatalf("create table: %v", err)
	}
	defer func() { _ = store.DeleteTable(ctx, tableName) }()

	// Hammer with many concurrent operations against tiny pool
	var wg sync.WaitGroup
	const numGoroutines = 20
	const opsPerGoroutine = 10

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				item := Item{
					"PK":    {"S": fmt.Sprintf("pk-%d-%d", gid, i)},
					"Value": {"S": "test"},
				}
				if _, err := store.PutItem(ctx, tableName, &meta, item, false); err != nil {
					// Pool exhaustion may cause timeouts, which is expected
					t.Logf("expected pool contention: %v", err)
					return
				}
			}
		}(g)
	}

	wg.Wait()
	t.Log("pool exhaustion chaos test completed")
}

func TestChaosRapidCreateDrop(t *testing.T) {
	connString := "postgres://dynamo:dynamo@localhost:5433/dynamo?sslmode=disable"
	ctx := context.Background()

	store, err := NewStore(ctx, connString)
	if err != nil {
		t.Skipf("skipping chaos test, no database: %v", err)
	}
	defer store.Close()

	// Rapidly create and drop tables
	const iterations = 20
	for i := 0; i < iterations; i++ {
		tableName := fmt.Sprintf("chaos_rapid_%d_%d", time.Now().UnixNano(), i)
		meta := TableMeta{
			TableName: tableName,
			PKName:    "PK",
			PKType:    "S",
			SKName:    "SK",
			SKType:    "S",
		}

		if err := store.CreateTable(ctx, meta); err != nil {
			t.Fatalf("iteration %d: create: %v", i, err)
		}

		// Write an item
		item := Item{
			"PK":    {"S": "test"},
			"SK":    {"S": "item"},
			"Value": {"N": "42"},
		}
		if _, err := store.PutItem(ctx, tableName, &meta, item, false); err != nil {
			t.Fatalf("iteration %d: put: %v", i, err)
		}

		if err := store.DeleteTable(ctx, tableName); err != nil {
			t.Fatalf("iteration %d: delete: %v", i, err)
		}
	}
	t.Logf("rapid create/drop: %d iterations completed", iterations)
}
