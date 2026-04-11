package storage

import (
	"context"
	json "github.com/goccy/go-json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// AsyncIndexConfig configures the async index maintenance worker.
type AsyncIndexConfig struct {
	// PollInterval controls how often the worker polls for pending index updates.
	PollInterval time.Duration
	// BatchSize is the max number of WAL entries to process per batch.
	BatchSize int
}

// AsyncIndexWorker processes index updates asynchronously via a WAL table.
// Writes to the main table append entries to _dynamo_index_wal.
// A background goroutine processes these entries and updates index tables.
type AsyncIndexWorker struct {
	pool   *pgxpool.Pool
	cfg    AsyncIndexConfig
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewAsyncIndexWorker creates a new async index worker and ensures the WAL table exists.
func NewAsyncIndexWorker(ctx context.Context, pool *pgxpool.Pool, cfg AsyncIndexConfig) (*AsyncIndexWorker, error) {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 100 * time.Millisecond
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}

	_, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS _dynamo_index_wal (
			id          BIGSERIAL PRIMARY KEY,
			table_name  TEXT NOT NULL,
			operation   TEXT NOT NULL,
			base_pk     TEXT NOT NULL,
			base_sk     TEXT NOT NULL DEFAULT '',
			item_json   JSONB,
			created_at  TIMESTAMPTZ DEFAULT NOW()
		)`)
	if err != nil {
		return nil, fmt.Errorf("create WAL table: %w", err)
	}

	// Index for efficient polling
	_, err = pool.Exec(ctx, `
		CREATE INDEX IF NOT EXISTS _dynamo_index_wal_pending ON _dynamo_index_wal (id)`)
	if err != nil {
		return nil, fmt.Errorf("create WAL index: %w", err)
	}

	return &AsyncIndexWorker{
		pool:   pool,
		cfg:    cfg,
		stopCh: make(chan struct{}),
	}, nil
}

// EnqueueUpdate writes an index update entry to the WAL table.
// operation is "PUT" or "DELETE".
func (w *AsyncIndexWorker) EnqueueUpdate(ctx context.Context, tableName, operation, basePK, baseSK string, item Item) error {
	var itemJSON []byte
	if item != nil {
		var err error
		itemJSON, err = json.Marshal(item)
		if err != nil {
			return fmt.Errorf("marshal item for WAL: %w", err)
		}
	}

	_, err := w.pool.Exec(ctx, `
		INSERT INTO _dynamo_index_wal (table_name, operation, base_pk, base_sk, item_json)
		VALUES ($1, $2, $3, $4, $5)`,
		tableName, operation, basePK, baseSK, itemJSON)
	return err
}

// Start begins the background worker goroutine.
func (w *AsyncIndexWorker) Start() {
	w.wg.Add(1)
	go w.run()
}

// Stop gracefully stops the background worker and waits for it to finish.
func (w *AsyncIndexWorker) Stop() {
	close(w.stopCh)
	w.wg.Wait()
}

func (w *AsyncIndexWorker) run() {
	defer w.wg.Done()

	ticker := time.NewTicker(w.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.stopCh:
			// Drain remaining entries before stopping
			w.processBatch()
			return
		case <-ticker.C:
			w.processBatch()
		}
	}
}

func (w *AsyncIndexWorker) processBatch() {
	ctx := context.Background()

	tx, err := w.pool.Begin(ctx)
	if err != nil {
		log.Printf("async index worker: begin tx: %v", err)
		return
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is expected to fail

	// Lock and fetch a batch of WAL entries
	rows, err := tx.Query(ctx, `
		DELETE FROM _dynamo_index_wal
		WHERE id IN (
			SELECT id FROM _dynamo_index_wal
			ORDER BY id
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING table_name, operation, base_pk, base_sk, item_json`,
		w.cfg.BatchSize)
	if err != nil {
		log.Printf("async index worker: query WAL: %v", err)
		return
	}

	type walEntry struct {
		tableName string
		operation string
		basePK    string
		baseSK    string
		itemJSON  []byte
	}

	entries := make([]walEntry, 0, w.cfg.BatchSize)
	for rows.Next() {
		var e walEntry
		if err := rows.Scan(&e.tableName, &e.operation, &e.basePK, &e.baseSK, &e.itemJSON); err != nil {
			log.Printf("async index worker: scan row: %v", err)
			rows.Close()
			return
		}
		entries = append(entries, e)
	}
	rows.Close()

	if len(entries) == 0 {
		return
	}

	// Process entries - build a batch of index operations
	batch := &pgx.Batch{}

	// Cache table metadata to avoid repeated lookups
	metaCache := make(map[string]*TableMeta)

	for _, e := range entries {
		meta, ok := metaCache[e.tableName]
		if !ok {
			var m TableMeta
			var skName, skType *string
			err := tx.QueryRow(ctx, `
				SELECT table_name, pk_name, pk_type, sk_name, sk_type, table_status,
				       EXTRACT(EPOCH FROM created_at)
				FROM _dynamo_tables WHERE table_name = $1`, e.tableName).
				Scan(&m.TableName, &m.PKName, &m.PKType, &skName, &skType, &m.TableStatus, &m.CreatedAt)
			if err != nil {
				continue
			}
			if skName != nil {
				m.SKName = *skName
			}
			if skType != nil {
				m.SKType = *skType
			}

			// Load indexes
			idxRows, err := tx.Query(ctx, `
				SELECT index_name, index_type, pk_name, pk_type, sk_name, sk_type, projection_type, non_key_attrs
				FROM _dynamo_indexes WHERE table_name = $1`, e.tableName)
			if err == nil {
				for idxRows.Next() {
					var idx IndexMeta
					var iSKName, iSKType, iProjType *string
					var nonKeyAttrs []string
					if err := idxRows.Scan(&idx.IndexName, &idx.IndexType, &idx.PKName, &idx.PKType,
						&iSKName, &iSKType, &iProjType, &nonKeyAttrs); err != nil {
						continue
					}
					if iSKName != nil {
						idx.SKName = *iSKName
					}
					if iSKType != nil {
						idx.SKType = *iSKType
					}
					if iProjType != nil {
						idx.ProjectionType = *iProjType
					}
					idx.NonKeyAttrs = nonKeyAttrs
					if idx.IndexType == "GSI" {
						m.GSIs = append(m.GSIs, idx)
					} else {
						m.LSIs = append(m.LSIs, idx)
					}
				}
				idxRows.Close()
			}

			meta = &m
			metaCache[e.tableName] = meta
		}

		allIndexes := make([]IndexMeta, 0, len(meta.GSIs)+len(meta.LSIs))
		allIndexes = append(allIndexes, meta.GSIs...)
		allIndexes = append(allIndexes, meta.LSIs...)
		if len(allIndexes) == 0 {
			continue
		}

		switch e.operation {
		case "PUT":
			var item Item
			if err := json.Unmarshal(e.itemJSON, &item); err != nil {
				continue
			}

			for _, idx := range allIndexes {
				idxTable := indexTableName(e.tableName, idx.IndexName)
				idxPK, err := extractKeyFromItem(item, idx.PKName, idx.PKType)
				if err != nil {
					batch.Queue(fmt.Sprintf(`DELETE FROM %q WHERE base_pk = $1 AND base_sk = $2`, idxTable), e.basePK, e.baseSK)
					continue
				}
				idxSK := ""
				if idx.SKName != "" {
					idxSK, err = extractKeyFromItem(item, idx.SKName, idx.SKType)
					if err != nil {
						batch.Queue(fmt.Sprintf(`DELETE FROM %q WHERE base_pk = $1 AND base_sk = $2`, idxTable), e.basePK, e.baseSK)
						continue
					}
				}

				projected := projectForIndex(item, idx, meta)
				projJSON, _ := json.Marshal(projected)

				batch.Queue(fmt.Sprintf(`
					INSERT INTO %q (pk, sk, base_pk, base_sk, attributes) VALUES ($1, $2, $3, $4, $5)
					ON CONFLICT (base_pk, base_sk) DO UPDATE SET pk = EXCLUDED.pk, sk = EXCLUDED.sk, attributes = EXCLUDED.attributes`,
					idxTable),
					idxPK, idxSK, e.basePK, e.baseSK, projJSON)
			}

		case "DELETE":
			for _, idx := range allIndexes {
				idxTable := indexTableName(e.tableName, idx.IndexName)
				batch.Queue(fmt.Sprintf(`DELETE FROM %q WHERE base_pk = $1 AND base_sk = $2`, idxTable), e.basePK, e.baseSK)
			}
		}
	}

	if batch.Len() > 0 {
		br := tx.SendBatch(ctx, batch)
		for i := 0; i < batch.Len(); i++ {
			_, _ = br.Exec()
		}
		_ = br.Close()
	}

	if err := tx.Commit(ctx); err != nil {
		log.Printf("async index worker: commit: %v", err)
	}
}
