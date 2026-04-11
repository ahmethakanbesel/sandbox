package storage

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	json "github.com/goccy/go-json"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// identifierRegex validates SQL identifiers to prevent injection.
var identifierRegex = regexp.MustCompile(`^[a-zA-Z0-9_.\-]+$`)

// validateIdentifier checks that a name is safe for use as a SQL identifier.
func validateIdentifier(name string) error {
	if name == "" || len(name) > 255 || !identifierRegex.MatchString(name) {
		return fmt.Errorf("ValidationException: invalid identifier: %q", name)
	}
	return nil
}

// Querier is the common interface between *pgxpool.Pool and pgx.Tx.
type Querier interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, arguments ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, arguments ...any) pgx.Row
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

// TableMeta holds DynamoDB table metadata stored in _dynamo_tables.
type TableMeta struct {
	TableName   string
	PKName      string
	PKType      string // S, N, B
	SKName      string // empty if no sort key
	SKType      string
	TableStatus string
	CreatedAt   float64
	// Index definitions stored here for convenience
	GSIs []IndexMeta
	LSIs []IndexMeta
}

// IndexMeta holds secondary index metadata.
type IndexMeta struct {
	IndexName      string
	IndexType      string // GSI or LSI
	PKName         string
	PKType         string
	SKName         string
	SKType         string
	ProjectionType string // ALL, KEYS_ONLY, INCLUDE
	NonKeyAttrs    []string
}

// QueryParams holds parameters for a DynamoDB Query operation.
type QueryParams struct {
	TableName        string
	IndexName        string // empty for base table
	PKValue          string
	SKCondition      string
	SKArgs           []any
	FilterSQL        string
	FilterArgs       []any
	Limit            int
	ScanIndexForward bool
	ExclusiveStartPK string
	ExclusiveStartSK string
	SKType           string
	SelectCount      bool
}

// ScanParams holds parameters for a DynamoDB Scan operation.
type ScanParams struct {
	TableName        string
	IndexName        string
	FilterSQL        string
	FilterArgs       []any
	Limit            int
	ExclusiveStartPK string
	ExclusiveStartSK string
	SKType           string
	SelectCount      bool
}

// QueryResult holds the result of a Query or Scan operation.
type QueryResult struct {
	Items           []Item
	Count           int
	ScannedCount    int
	LastEvaluatedPK string
	LastEvaluatedSK string
}

// StoreConfig holds configuration for the Store connection pool.
type StoreConfig struct {
	ConnString      string
	MaxConns        int32
	MinConns        int32
	MaxConnLifetime time.Duration
	MaxConnIdleTime time.Duration
	// EnableMetaCache enables in-memory caching of table metadata.
	// This avoids 2 PostgreSQL round trips per operation but means
	// metadata changes made outside this process won't be seen
	// until the cache is invalidated (via CreateTable/DeleteTable/UpdateTable).
	EnableMetaCache bool
}

// MetaCache is the interface for table metadata caching.
type MetaCache interface {
	Get(tableName string) (*TableMeta, bool)
	Set(tableName string, meta *TableMeta)
	Invalidate(tableName string)
}

// noopCache is the default no-op implementation that never caches.
type noopCache struct{}

func (noopCache) Get(string) (*TableMeta, bool) { return nil, false }
func (noopCache) Set(string, *TableMeta)        {}
func (noopCache) Invalidate(string)              {}

// InMemCache provides a thread-safe in-memory metadata cache.
type InMemCache struct {
	mu    sync.RWMutex
	items map[string]*TableMeta
}

// NewInMemCache creates a new in-memory metadata cache.
func NewInMemCache() *InMemCache {
	return &InMemCache{items: make(map[string]*TableMeta)}
}

func (c *InMemCache) Get(tableName string) (*TableMeta, bool) {
	c.mu.RLock()
	meta, ok := c.items[tableName]
	c.mu.RUnlock()
	return meta, ok
}

func (c *InMemCache) Set(tableName string, meta *TableMeta) {
	c.mu.Lock()
	c.items[tableName] = meta
	c.mu.Unlock()
}

func (c *InMemCache) Invalidate(tableName string) {
	c.mu.Lock()
	delete(c.items, tableName)
	c.mu.Unlock()
}

// Store wraps a pgxpool.Pool and provides DynamoDB-like storage operations.
type Store struct {
	pool  *pgxpool.Pool
	cache MetaCache
}

// NewStore creates a new Store with default connection pool settings.
func NewStore(ctx context.Context, connString string) (*Store, error) {
	return NewStoreWithConfig(ctx, StoreConfig{ConnString: connString})
}

// NewStoreWithConfig creates a new Store with custom connection pool settings.
func NewStoreWithConfig(ctx context.Context, cfg StoreConfig) (*Store, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.ConnString)
	if err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if cfg.MaxConns > 0 {
		poolCfg.MaxConns = cfg.MaxConns
	}
	if cfg.MinConns > 0 {
		poolCfg.MinConns = cfg.MinConns
	}
	if cfg.MaxConnLifetime > 0 {
		poolCfg.MaxConnLifetime = cfg.MaxConnLifetime
	}
	if cfg.MaxConnIdleTime > 0 {
		poolCfg.MaxConnIdleTime = cfg.MaxConnIdleTime
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	var cache MetaCache = noopCache{}
	if cfg.EnableMetaCache {
		cache = NewInMemCache()
	}
	s := &Store{pool: pool, cache: cache}
	if err := s.ensureMetaTables(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

// Close closes the connection pool.
func (s *Store) Close() {
	s.pool.Close()
}

// BeginTx starts a new transaction.
func (s *Store) BeginTx(ctx context.Context) (pgx.Tx, error) {
	return s.pool.Begin(ctx)
}

// InvalidateTableMeta removes a table's cached metadata, forcing a fresh read on next access.
// This is a no-op when metadata caching is disabled.
func (s *Store) InvalidateTableMeta(tableName string) {
	s.cache.Invalidate(tableName)
}

func (s *Store) ensureMetaTables(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS _dynamo_tables (
			table_name   TEXT PRIMARY KEY,
			pk_name      TEXT NOT NULL,
			pk_type      TEXT NOT NULL,
			sk_name      TEXT,
			sk_type      TEXT,
			table_status TEXT DEFAULT 'ACTIVE',
			created_at   TIMESTAMPTZ DEFAULT NOW()
		)`)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS _dynamo_indexes (
			table_name      TEXT NOT NULL,
			index_name      TEXT NOT NULL,
			index_type      TEXT NOT NULL,
			pk_name         TEXT NOT NULL,
			pk_type         TEXT NOT NULL,
			sk_name         TEXT,
			sk_type         TEXT,
			projection_type TEXT DEFAULT 'ALL',
			non_key_attrs   TEXT[],
			PRIMARY KEY (table_name, index_name)
		)`)
	return err
}

func dataTableName(tableName string) string {
	return "dyn_" + tableName
}

func indexTableName(tableName, indexName string) string {
	return "dyn_" + tableName + "_idx_" + indexName
}

// CreateTable creates a DynamoDB table backed by a PostgreSQL table.
func (s *Store) CreateTable(ctx context.Context, meta TableMeta) error {
	if err := validateIdentifier(meta.TableName); err != nil {
		return err
	}
	for _, gsi := range meta.GSIs {
		if err := validateIdentifier(gsi.IndexName); err != nil {
			return err
		}
	}
	for _, lsi := range meta.LSIs {
		if err := validateIdentifier(lsi.IndexName); err != nil {
			return err
		}
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is expected to fail

	_, err = tx.Exec(ctx, `
		INSERT INTO _dynamo_tables (table_name, pk_name, pk_type, sk_name, sk_type, table_status)
		VALUES ($1, $2, $3, $4, $5, 'ACTIVE')`,
		meta.TableName, meta.PKName, meta.PKType, nilIfEmpty(meta.SKName), nilIfEmpty(meta.SKType))
	if err != nil {
		if strings.Contains(err.Error(), "duplicate key") {
			return fmt.Errorf("table already exists: %s", meta.TableName)
		}
		return fmt.Errorf("insert metadata: %w", err)
	}

	dtName := dataTableName(meta.TableName)
	_, err = tx.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %q (
			pk         TEXT NOT NULL,
			sk         TEXT NOT NULL DEFAULT '',
			attributes JSONB NOT NULL DEFAULT '{}',
			PRIMARY KEY (pk, sk)
		)`, dtName))
	if err != nil {
		return fmt.Errorf("create data table: %w", err)
	}

	// GIN index on attributes for JSONB containment queries
	_, err = tx.Exec(ctx, fmt.Sprintf(
		`CREATE INDEX IF NOT EXISTS %q ON %q USING gin (attributes)`,
		dtName+"_attrs_gin", dtName))
	if err != nil {
		return fmt.Errorf("create GIN index: %w", err)
	}

	// Hash index on pk for fast equality lookups
	_, err = tx.Exec(ctx, fmt.Sprintf(
		`CREATE INDEX IF NOT EXISTS %q ON %q USING hash (pk)`,
		dtName+"_pk_hash", dtName))
	if err != nil {
		return fmt.Errorf("create hash index: %w", err)
	}

	// Functional index for numeric sort key ordering
	if meta.SKType == "N" {
		_, err = tx.Exec(ctx, fmt.Sprintf(
			`CREATE INDEX IF NOT EXISTS %q ON %q (pk, CAST(sk AS NUMERIC))`,
			dtName+"_numeric_sk", dtName))
		if err != nil {
			return fmt.Errorf("create numeric SK index: %w", err)
		}
	}

	// Create GSI tables
	for _, gsi := range meta.GSIs {
		if err := s.createIndexTable(ctx, tx, meta.TableName, gsi); err != nil {
			return err
		}
	}

	// Create LSI tables
	for _, lsi := range meta.LSIs {
		if err := s.createIndexTable(ctx, tx, meta.TableName, lsi); err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if s.cache != nil {
		s.cache.Invalidate(meta.TableName)
	}
	return nil
}

func (s *Store) createIndexTable(ctx context.Context, tx pgx.Tx, tableName string, idx IndexMeta) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO _dynamo_indexes (table_name, index_name, index_type, pk_name, pk_type, sk_name, sk_type, projection_type, non_key_attrs)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		tableName, idx.IndexName, idx.IndexType, idx.PKName, idx.PKType,
		nilIfEmpty(idx.SKName), nilIfEmpty(idx.SKType),
		idx.ProjectionType, idx.NonKeyAttrs)
	if err != nil {
		return fmt.Errorf("insert index metadata: %w", err)
	}

	idxTable := indexTableName(tableName, idx.IndexName)
	_, err = tx.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %q (
			pk         TEXT NOT NULL,
			sk         TEXT NOT NULL DEFAULT '',
			base_pk    TEXT NOT NULL,
			base_sk    TEXT NOT NULL DEFAULT '',
			attributes JSONB NOT NULL DEFAULT '{}'
		)`, idxTable))
	if err != nil {
		return fmt.Errorf("create index table: %w", err)
	}

	// B-tree index for efficient queries
	_, err = tx.Exec(ctx, fmt.Sprintf(`
		CREATE INDEX IF NOT EXISTS %q ON %q (pk, sk)`,
		idxTable+"_pk_sk", idxTable))
	if err != nil {
		return fmt.Errorf("create index: %w", err)
	}

	// Unique index on base keys for upsert during maintenance
	_, err = tx.Exec(ctx, fmt.Sprintf(`
		CREATE UNIQUE INDEX IF NOT EXISTS %q ON %q (base_pk, base_sk)`,
		idxTable+"_base", idxTable))
	if err != nil {
		return fmt.Errorf("create base index: %w", err)
	}

	// Hash index on pk for fast equality lookups
	_, err = tx.Exec(ctx, fmt.Sprintf(
		`CREATE INDEX IF NOT EXISTS %q ON %q USING hash (pk)`,
		idxTable+"_pk_hash", idxTable))
	if err != nil {
		return fmt.Errorf("create hash index: %w", err)
	}

	// Functional index for numeric sort key ordering
	if idx.SKType == "N" {
		_, err = tx.Exec(ctx, fmt.Sprintf(
			`CREATE INDEX IF NOT EXISTS %q ON %q (pk, CAST(sk AS NUMERIC))`,
			idxTable+"_numeric_sk", idxTable))
		if err != nil {
			return fmt.Errorf("create numeric SK index: %w", err)
		}
	}

	return nil
}

// DeleteTable drops the data table, index tables, and removes metadata.
func (s *Store) DeleteTable(ctx context.Context, tableName string) error {
	if err := validateIdentifier(tableName); err != nil {
		return err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is expected to fail

	tag, err := tx.Exec(ctx, `DELETE FROM _dynamo_tables WHERE table_name = $1`, tableName)
	if err != nil {
		return fmt.Errorf("delete metadata: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("table not found: %s", tableName)
	}

	// Drop index tables
	indexes, err := s.getIndexes(ctx, tx, tableName)
	if err == nil {
		for _, idx := range indexes {
			idxTable := indexTableName(tableName, idx.IndexName)
			_, _ = tx.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %q`, idxTable))
		}
	}

	_, _ = tx.Exec(ctx, `DELETE FROM _dynamo_indexes WHERE table_name = $1`, tableName)

	dtName := dataTableName(tableName)
	_, err = tx.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %q`, dtName))
	if err != nil {
		return fmt.Errorf("drop data table: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if s.cache != nil {
		s.cache.Invalidate(tableName)
	}
	return nil
}

// GetTableMeta retrieves metadata for a DynamoDB table including indexes.
// When metadata caching is enabled, results are served from an in-memory cache
// to avoid 2 PostgreSQL round trips per operation.
func (s *Store) GetTableMeta(ctx context.Context, tableName string) (*TableMeta, error) {
	if meta, ok := s.cache.Get(tableName); ok {
		return meta, nil
	}

	meta, err := s.getTableMetaFromDB(ctx, s.pool, tableName)
	if err != nil {
		return nil, err
	}

	s.cache.Set(tableName, meta)
	return meta, nil
}

func (s *Store) getTableMetaFromDB(ctx context.Context, q Querier, tableName string) (*TableMeta, error) {
	var m TableMeta
	var skName, skType *string
	err := q.QueryRow(ctx, `
		SELECT table_name, pk_name, pk_type, sk_name, sk_type, table_status,
		       EXTRACT(EPOCH FROM created_at)
		FROM _dynamo_tables WHERE table_name = $1`, tableName).
		Scan(&m.TableName, &m.PKName, &m.PKType, &skName, &skType, &m.TableStatus, &m.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("query table meta: %w", err)
	}
	if skName != nil {
		m.SKName = *skName
	}
	if skType != nil {
		m.SKType = *skType
	}

	indexes, err := s.getIndexes(ctx, q, tableName)
	if err != nil {
		return nil, err
	}
	for _, idx := range indexes {
		if idx.IndexType == "GSI" {
			m.GSIs = append(m.GSIs, idx)
		} else {
			m.LSIs = append(m.LSIs, idx)
		}
	}

	return &m, nil
}

func (s *Store) getIndexes(ctx context.Context, q Querier, tableName string) ([]IndexMeta, error) {
	rows, err := q.Query(ctx, `
		SELECT index_name, index_type, pk_name, pk_type, sk_name, sk_type, projection_type, non_key_attrs
		FROM _dynamo_indexes WHERE table_name = $1`, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	indexes := make([]IndexMeta, 0, 8)
	for rows.Next() {
		var idx IndexMeta
		var skName, skType *string
		var projType *string
		var nonKeyAttrs []string
		if err := rows.Scan(&idx.IndexName, &idx.IndexType, &idx.PKName, &idx.PKType, &skName, &skType, &projType, &nonKeyAttrs); err != nil {
			return nil, err
		}
		if skName != nil {
			idx.SKName = *skName
		}
		if skType != nil {
			idx.SKType = *skType
		}
		if projType != nil {
			idx.ProjectionType = *projType
		}
		idx.NonKeyAttrs = nonKeyAttrs
		indexes = append(indexes, idx)
	}
	return indexes, nil
}

// ListTables returns table names with cursor-based pagination.
func (s *Store) ListTables(ctx context.Context, exclusiveStart string, limit int) ([]string, string, error) {
	if limit <= 0 {
		limit = 100
	}
	var rows pgx.Rows
	var err error
	if exclusiveStart == "" {
		rows, err = s.pool.Query(ctx,
			`SELECT table_name FROM _dynamo_tables WHERE table_status = 'ACTIVE' ORDER BY table_name LIMIT $1`,
			limit+1)
	} else {
		rows, err = s.pool.Query(ctx,
			`SELECT table_name FROM _dynamo_tables WHERE table_status = 'ACTIVE' AND table_name > $1 ORDER BY table_name LIMIT $2`,
			exclusiveStart, limit+1)
	}
	if err != nil {
		return nil, "", fmt.Errorf("list tables: %w", err)
	}
	defer rows.Close()

	names := make([]string, 0, limit+1)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, "", err
		}
		names = append(names, name)
	}

	var lastEval string
	if len(names) > limit {
		lastEval = names[limit-1]
		names = names[:limit]
	}
	return names, lastEval, nil
}

// PutItem upserts an item. Returns the old item if returnOld is true.
func (s *Store) PutItem(ctx context.Context, tableName string, meta *TableMeta, item Item, returnOld bool) (Item, error) {
	return s.putItemWith(ctx, s.pool, tableName, meta, item, returnOld)
}

// PutItemTx is like PutItem but runs within a transaction.
func (s *Store) PutItemTx(ctx context.Context, tx pgx.Tx, tableName string, meta *TableMeta, item Item, returnOld bool) (Item, error) {
	return s.putItemWith(ctx, tx, tableName, meta, item, returnOld)
}

func (s *Store) putItemWith(ctx context.Context, q Querier, tableName string, meta *TableMeta, item Item, returnOld bool) (Item, error) {
	pk, err := ExtractKeyValue(item, meta.PKName, meta.PKType)
	if err != nil {
		return nil, err
	}
	sk := ""
	if meta.SKName != "" {
		sk, err = ExtractKeyValue(item, meta.SKName, meta.SKType)
		if err != nil {
			return nil, err
		}
	}

	attrsJSON, err := json.Marshal(item)
	if err != nil {
		return nil, fmt.Errorf("marshal attributes: %w", err)
	}

	dtName := dataTableName(tableName)
	var oldItem Item

	if returnOld {
		var oldJSON []byte
		err = q.QueryRow(ctx,
			fmt.Sprintf(`SELECT attributes FROM %q WHERE pk = $1 AND sk = $2`, dtName),
			pk, sk).Scan(&oldJSON)
		if err == nil {
			_ = json.Unmarshal(oldJSON, &oldItem)
		}
	}

	_, err = q.Exec(ctx,
		fmt.Sprintf(`INSERT INTO %q (pk, sk, attributes) VALUES ($1, $2, $3)
			ON CONFLICT (pk, sk) DO UPDATE SET attributes = $3`, dtName),
		pk, sk, attrsJSON)
	if err != nil {
		return nil, fmt.Errorf("upsert item: %w", err)
	}

	// Maintain indexes (pass pre-marshaled JSON for reuse)
	s.maintainIndexes(ctx, q, tableName, meta, pk, sk, item, attrsJSON)

	return oldItem, nil
}

// GetItem retrieves a single item by primary key.
func (s *Store) GetItem(ctx context.Context, tableName string, meta *TableMeta, key Item) (Item, error) {
	return s.getItemWith(ctx, s.pool, tableName, meta, key)
}

// GetItemTx is like GetItem but runs within a transaction.
func (s *Store) GetItemTx(ctx context.Context, tx pgx.Tx, tableName string, meta *TableMeta, key Item) (Item, error) {
	return s.getItemWith(ctx, tx, tableName, meta, key)
}

func (s *Store) getItemWith(ctx context.Context, q Querier, tableName string, meta *TableMeta, key Item) (Item, error) {
	pk, err := ExtractKeyValue(key, meta.PKName, meta.PKType)
	if err != nil {
		return nil, err
	}
	sk := ""
	if meta.SKName != "" {
		sk, err = ExtractKeyValue(key, meta.SKName, meta.SKType)
		if err != nil {
			return nil, err
		}
	}

	dtName := dataTableName(tableName)
	var attrsJSON []byte
	err = q.QueryRow(ctx,
		fmt.Sprintf(`SELECT attributes FROM %q WHERE pk = $1 AND sk = $2`, dtName),
		pk, sk).Scan(&attrsJSON)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get item: %w", err)
	}

	var item Item
	if err := json.Unmarshal(attrsJSON, &item); err != nil {
		return nil, fmt.Errorf("unmarshal item: %w", err)
	}
	return item, nil
}

// DeleteItem deletes an item by primary key. Returns the old item if returnOld is true.
func (s *Store) DeleteItem(ctx context.Context, tableName string, meta *TableMeta, key Item, returnOld bool) (Item, error) {
	return s.deleteItemWith(ctx, s.pool, tableName, meta, key, returnOld)
}

// DeleteItemTx is like DeleteItem but runs within a transaction.
func (s *Store) DeleteItemTx(ctx context.Context, tx pgx.Tx, tableName string, meta *TableMeta, key Item, returnOld bool) (Item, error) {
	return s.deleteItemWith(ctx, tx, tableName, meta, key, returnOld)
}

func (s *Store) deleteItemWith(ctx context.Context, q Querier, tableName string, meta *TableMeta, key Item, returnOld bool) (Item, error) {
	pk, err := ExtractKeyValue(key, meta.PKName, meta.PKType)
	if err != nil {
		return nil, err
	}
	sk := ""
	if meta.SKName != "" {
		sk, err = ExtractKeyValue(key, meta.SKName, meta.SKType)
		if err != nil {
			return nil, err
		}
	}

	dtName := dataTableName(tableName)

	if returnOld {
		var attrsJSON []byte
		err = q.QueryRow(ctx,
			fmt.Sprintf(`DELETE FROM %q WHERE pk = $1 AND sk = $2 RETURNING attributes`, dtName),
			pk, sk).Scan(&attrsJSON)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, nil
			}
			return nil, fmt.Errorf("delete item: %w", err)
		}
		var item Item
		if err := json.Unmarshal(attrsJSON, &item); err != nil {
			return nil, fmt.Errorf("unmarshal item: %w", err)
		}
		// Remove from indexes
		s.removeFromIndexes(ctx, q, tableName, meta, pk, sk)
		return item, nil
	}

	_, err = q.Exec(ctx,
		fmt.Sprintf(`DELETE FROM %q WHERE pk = $1 AND sk = $2`, dtName),
		pk, sk)
	if err != nil {
		return nil, fmt.Errorf("delete item: %w", err)
	}
	s.removeFromIndexes(ctx, q, tableName, meta, pk, sk)
	return nil, nil
}

// UpdateItem updates an item in place. Returns old/new item based on returnValues.
func (s *Store) UpdateItem(ctx context.Context, tableName string, meta *TableMeta, key Item, newItem Item, returnOld bool) (Item, error) {
	return s.updateItemWith(ctx, s.pool, tableName, meta, key, newItem, returnOld)
}

// UpdateItemTx is like UpdateItem but runs within a transaction.
func (s *Store) UpdateItemTx(ctx context.Context, tx pgx.Tx, tableName string, meta *TableMeta, key Item, newItem Item, returnOld bool) (Item, error) {
	return s.updateItemWith(ctx, tx, tableName, meta, key, newItem, returnOld)
}

func (s *Store) updateItemWith(ctx context.Context, q Querier, tableName string, meta *TableMeta, key Item, newItem Item, returnOld bool) (Item, error) {
	pk, err := ExtractKeyValue(key, meta.PKName, meta.PKType)
	if err != nil {
		return nil, err
	}
	sk := ""
	if meta.SKName != "" {
		sk, err = ExtractKeyValue(key, meta.SKName, meta.SKType)
		if err != nil {
			return nil, err
		}
	}

	attrsJSON, err := json.Marshal(newItem)
	if err != nil {
		return nil, fmt.Errorf("marshal attributes: %w", err)
	}

	dtName := dataTableName(tableName)

	if returnOld {
		var oldJSON []byte
		err = q.QueryRow(ctx,
			fmt.Sprintf(`SELECT attributes FROM %q WHERE pk = $1 AND sk = $2`, dtName),
			pk, sk).Scan(&oldJSON)
		var oldItem Item
		if err == nil {
			_ = json.Unmarshal(oldJSON, &oldItem)
		}

		_, err = q.Exec(ctx,
			fmt.Sprintf(`INSERT INTO %q (pk, sk, attributes) VALUES ($1, $2, $3)
				ON CONFLICT (pk, sk) DO UPDATE SET attributes = $3`, dtName),
			pk, sk, attrsJSON)
		if err != nil {
			return nil, fmt.Errorf("update item: %w", err)
		}

		s.maintainIndexes(ctx, q, tableName, meta, pk, sk, newItem, attrsJSON)
		return oldItem, nil
	}

	_, err = q.Exec(ctx,
		fmt.Sprintf(`INSERT INTO %q (pk, sk, attributes) VALUES ($1, $2, $3)
			ON CONFLICT (pk, sk) DO UPDATE SET attributes = $3`, dtName),
		pk, sk, attrsJSON)
	if err != nil {
		return nil, fmt.Errorf("update item: %w", err)
	}

	s.maintainIndexes(ctx, q, tableName, meta, pk, sk, newItem, attrsJSON)
	return newItem, nil
}

// maintainIndexes updates all secondary index tables after a write to the base table.
// Uses pgx.Batch to send all index updates in a single network round-trip.
// Reuses pre-marshaled attrsJSON for ALL-projection indexes to avoid redundant marshaling.
// Uses IS DISTINCT FROM to skip unchanged index rows (avoids dead tuples).
func (s *Store) maintainIndexes(ctx context.Context, q Querier, tableName string, meta *TableMeta, basePK, baseSK string, item Item, attrsJSON []byte) {
	allIndexes := make([]IndexMeta, 0, len(meta.GSIs)+len(meta.LSIs))
	allIndexes = append(allIndexes, meta.GSIs...)
	allIndexes = append(allIndexes, meta.LSIs...)
	if len(allIndexes) == 0 {
		return
	}

	batch := &pgx.Batch{}
	for _, idx := range allIndexes {
		idxTable := indexTableName(tableName, idx.IndexName)

		// Extract index PK value from item
		idxPK, err := extractKeyFromItem(item, idx.PKName, idx.PKType)
		if err != nil {
			// Item doesn't have the index PK attribute — remove from index
			batch.Queue(fmt.Sprintf(`DELETE FROM %q WHERE base_pk = $1 AND base_sk = $2`, idxTable), basePK, baseSK)
			continue
		}

		idxSK := ""
		if idx.SKName != "" {
			idxSK, err = extractKeyFromItem(item, idx.SKName, idx.SKType)
			if err != nil {
				batch.Queue(fmt.Sprintf(`DELETE FROM %q WHERE base_pk = $1 AND base_sk = $2`, idxTable), basePK, baseSK)
				continue
			}
		}

		// Project attributes — reuse pre-marshaled JSON for ALL projection
		var projJSON []byte
		if idx.ProjectionType == "ALL" || idx.ProjectionType == "" {
			projJSON = attrsJSON
		} else {
			projected := projectForIndex(item, idx, meta)
			projJSON, _ = json.Marshal(projected)
		}

		// Skip unchanged rows using IS DISTINCT FROM to avoid dead tuples
		batch.Queue(fmt.Sprintf(`
			INSERT INTO %q (pk, sk, base_pk, base_sk, attributes) VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (base_pk, base_sk) DO UPDATE SET pk = EXCLUDED.pk, sk = EXCLUDED.sk, attributes = EXCLUDED.attributes
			WHERE %q.pk IS DISTINCT FROM EXCLUDED.pk
			   OR %q.sk IS DISTINCT FROM EXCLUDED.sk
			   OR %q.attributes IS DISTINCT FROM EXCLUDED.attributes`, idxTable, idxTable, idxTable, idxTable),
			idxPK, idxSK, basePK, baseSK, projJSON)
	}

	if batch.Len() > 0 {
		br := q.SendBatch(ctx, batch)
		// Consume all results to complete the batch
		for i := 0; i < batch.Len(); i++ {
			_, _ = br.Exec()
		}
		_ = br.Close()
	}
}

// removeFromIndexes deletes entries from all index tables using pgx.Batch.
func (s *Store) removeFromIndexes(ctx context.Context, q Querier, tableName string, meta *TableMeta, basePK, baseSK string) {
	allIndexes := make([]IndexMeta, 0, len(meta.GSIs)+len(meta.LSIs))
	allIndexes = append(allIndexes, meta.GSIs...)
	allIndexes = append(allIndexes, meta.LSIs...)
	if len(allIndexes) == 0 {
		return
	}

	batch := &pgx.Batch{}
	for _, idx := range allIndexes {
		idxTable := indexTableName(tableName, idx.IndexName)
		batch.Queue(fmt.Sprintf(`DELETE FROM %q WHERE base_pk = $1 AND base_sk = $2`, idxTable), basePK, baseSK)
	}

	br := q.SendBatch(ctx, batch)
	for i := 0; i < batch.Len(); i++ {
		_, _ = br.Exec()
	}
	_ = br.Close()
}

func extractKeyFromItem(item Item, attrName, attrType string) (string, error) {
	av, ok := item[attrName]
	if !ok {
		return "", fmt.Errorf("missing attribute %s", attrName)
	}
	raw, ok := av[attrType]
	if !ok {
		return "", fmt.Errorf("attribute %s not of type %s", attrName, attrType)
	}
	s, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf("attribute %s value not string", attrName)
	}
	return s, nil
}

func projectForIndex(item Item, idx IndexMeta, meta *TableMeta) Item {
	switch idx.ProjectionType {
	case "ALL", "":
		return item
	case "KEYS_ONLY":
		result := make(Item)
		// Base table keys
		if v, ok := item[meta.PKName]; ok {
			result[meta.PKName] = v
		}
		if meta.SKName != "" {
			if v, ok := item[meta.SKName]; ok {
				result[meta.SKName] = v
			}
		}
		// Index keys
		if v, ok := item[idx.PKName]; ok {
			result[idx.PKName] = v
		}
		if idx.SKName != "" {
			if v, ok := item[idx.SKName]; ok {
				result[idx.SKName] = v
			}
		}
		return result
	case "INCLUDE":
		result := projectForIndex(item, IndexMeta{
			PKName: idx.PKName, PKType: idx.PKType,
			SKName: idx.SKName, SKType: idx.SKType,
			ProjectionType: "KEYS_ONLY",
		}, meta)
		for _, attr := range idx.NonKeyAttrs {
			if v, ok := item[attr]; ok {
				result[attr] = v
			}
		}
		return result
	}
	return item
}

// BatchPutItems inserts multiple items in a single multi-row INSERT statement.
func (s *Store) BatchPutItems(ctx context.Context, tableName string, meta *TableMeta, items []Item) error {
	if len(items) == 0 {
		return nil
	}

	dtName := dataTableName(tableName)
	args := make([]any, 0, len(items)*3)
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf(`INSERT INTO %q (pk, sk, attributes) VALUES `, dtName))

	for i, item := range items {
		pk, err := ExtractKeyValue(item, meta.PKName, meta.PKType)
		if err != nil {
			return err
		}
		sk := ""
		if meta.SKName != "" {
			sk, err = ExtractKeyValue(item, meta.SKName, meta.SKType)
			if err != nil {
				return err
			}
		}
		attrsJSON, err := json.Marshal(item)
		if err != nil {
			return fmt.Errorf("marshal item: %w", err)
		}

		if i > 0 {
			sb.WriteString(", ")
		}
		base := i * 3
		fmt.Fprintf(&sb, "($%d, $%d, $%d)", base+1, base+2, base+3)
		args = append(args, pk, sk, attrsJSON)
	}

	sb.WriteString(` ON CONFLICT (pk, sk) DO UPDATE SET attributes = EXCLUDED.attributes`)

	_, err := s.pool.Exec(ctx, sb.String(), args...)
	if err != nil {
		return fmt.Errorf("batch put items: %w", err)
	}

	// Maintain indexes for all items using batch
	s.batchMaintainIndexes(ctx, tableName, meta, items)
	return nil
}

// BatchDeleteItems deletes multiple items in a single statement.
func (s *Store) BatchDeleteItems(ctx context.Context, tableName string, meta *TableMeta, keys []Item) error {
	if len(keys) == 0 {
		return nil
	}

	dtName := dataTableName(tableName)
	batch := &pgx.Batch{}

	for _, key := range keys {
		pk, err := ExtractKeyValue(key, meta.PKName, meta.PKType)
		if err != nil {
			return err
		}
		sk := ""
		if meta.SKName != "" {
			sk, err = ExtractKeyValue(key, meta.SKName, meta.SKType)
			if err != nil {
				return err
			}
		}
		batch.Queue(fmt.Sprintf(`DELETE FROM %q WHERE pk = $1 AND sk = $2`, dtName), pk, sk)

		// Queue index removals
		allIndexes := make([]IndexMeta, 0, len(meta.GSIs)+len(meta.LSIs))
		allIndexes = append(allIndexes, meta.GSIs...)
		allIndexes = append(allIndexes, meta.LSIs...)
		for _, idx := range allIndexes {
			idxTable := indexTableName(tableName, idx.IndexName)
			batch.Queue(fmt.Sprintf(`DELETE FROM %q WHERE base_pk = $1 AND base_sk = $2`, idxTable), pk, sk)
		}
	}

	br := s.pool.SendBatch(ctx, batch)
	for i := 0; i < batch.Len(); i++ {
		_, _ = br.Exec()
	}
	_ = br.Close()
	return nil
}

// batchMaintainIndexes maintains indexes for multiple items using pgx.Batch.
func (s *Store) batchMaintainIndexes(ctx context.Context, tableName string, meta *TableMeta, items []Item) {
	allIndexes := make([]IndexMeta, 0, len(meta.GSIs)+len(meta.LSIs))
	allIndexes = append(allIndexes, meta.GSIs...)
	allIndexes = append(allIndexes, meta.LSIs...)
	if len(allIndexes) == 0 {
		return
	}

	batch := &pgx.Batch{}
	for _, item := range items {
		pk, _ := ExtractKeyValue(item, meta.PKName, meta.PKType)
		sk := ""
		if meta.SKName != "" {
			sk, _ = ExtractKeyValue(item, meta.SKName, meta.SKType)
		}
		attrsJSON, _ := json.Marshal(item)

		for _, idx := range allIndexes {
			idxTable := indexTableName(tableName, idx.IndexName)

			idxPK, err := extractKeyFromItem(item, idx.PKName, idx.PKType)
			if err != nil {
				batch.Queue(fmt.Sprintf(`DELETE FROM %q WHERE base_pk = $1 AND base_sk = $2`, idxTable), pk, sk)
				continue
			}
			idxSK := ""
			if idx.SKName != "" {
				idxSK, err = extractKeyFromItem(item, idx.SKName, idx.SKType)
				if err != nil {
					batch.Queue(fmt.Sprintf(`DELETE FROM %q WHERE base_pk = $1 AND base_sk = $2`, idxTable), pk, sk)
					continue
				}
			}

			var projJSON []byte
			if idx.ProjectionType == "ALL" || idx.ProjectionType == "" {
				projJSON = attrsJSON
			} else {
				projected := projectForIndex(item, idx, meta)
				projJSON, _ = json.Marshal(projected)
			}

			batch.Queue(fmt.Sprintf(`
				INSERT INTO %q (pk, sk, base_pk, base_sk, attributes) VALUES ($1, $2, $3, $4, $5)
				ON CONFLICT (base_pk, base_sk) DO UPDATE SET pk = EXCLUDED.pk, sk = EXCLUDED.sk, attributes = EXCLUDED.attributes
				WHERE %q.pk IS DISTINCT FROM EXCLUDED.pk
				   OR %q.sk IS DISTINCT FROM EXCLUDED.sk
				   OR %q.attributes IS DISTINCT FROM EXCLUDED.attributes`, idxTable, idxTable, idxTable, idxTable),
				idxPK, idxSK, pk, sk, projJSON)
		}
	}

	if batch.Len() > 0 {
		br := s.pool.SendBatch(ctx, batch)
		for i := 0; i < batch.Len(); i++ {
			_, _ = br.Exec()
		}
		_ = br.Close()
	}
}

// Query executes a DynamoDB-style Query against PostgreSQL.
func (s *Store) Query(ctx context.Context, p QueryParams) (*QueryResult, error) {
	if err := validateIdentifier(p.TableName); err != nil {
		return nil, err
	}
	if p.IndexName != "" {
		if err := validateIdentifier(p.IndexName); err != nil {
			return nil, err
		}
	}

	tableName := dataTableName(p.TableName)
	if p.IndexName != "" {
		tableName = indexTableName(p.TableName, p.IndexName)
	}

	argIdx := 1
	args := []any{p.PKValue}

	where := fmt.Sprintf("pk = $%d", argIdx)
	argIdx++

	if p.SKCondition != "" {
		where += " AND " + p.SKCondition
		args = append(args, p.SKArgs...)
		argIdx += len(p.SKArgs)
	}

	if p.ExclusiveStartPK != "" {
		if p.ScanIndexForward {
			where += fmt.Sprintf(" AND (pk, sk) > ($%d, $%d)", argIdx, argIdx+1)
		} else {
			where += fmt.Sprintf(" AND (pk, sk) < ($%d, $%d)", argIdx, argIdx+1)
		}
		args = append(args, p.ExclusiveStartPK, p.ExclusiveStartSK)
	}

	orderCol := "sk"
	if p.SKType == "N" {
		orderCol = "CAST(sk AS NUMERIC)"
	}
	dir := "ASC"
	if !p.ScanIndexForward {
		dir = "DESC"
	}

	if p.SelectCount && p.FilterSQL == "" {
		q := fmt.Sprintf(`SELECT COUNT(*) FROM %q WHERE %s`, tableName, where)
		var count int
		if err := s.pool.QueryRow(ctx, q, args...).Scan(&count); err != nil {
			return nil, fmt.Errorf("query count: %w", err)
		}
		return &QueryResult{Count: count, ScannedCount: count}, nil
	}

	limitClause := ""
	if p.Limit > 0 {
		limitClause = fmt.Sprintf(" LIMIT %d", p.Limit)
	}

	q := fmt.Sprintf(`SELECT pk, sk, attributes FROM %q WHERE %s ORDER BY %s %s%s`,
		tableName, where, orderCol, dir, limitClause)

	return s.executeItemQuery(ctx, q, args, p.Limit, p.SelectCount)
}

// Scan executes a DynamoDB-style Scan against PostgreSQL.
func (s *Store) Scan(ctx context.Context, p ScanParams) (*QueryResult, error) {
	if err := validateIdentifier(p.TableName); err != nil {
		return nil, err
	}
	if p.IndexName != "" {
		if err := validateIdentifier(p.IndexName); err != nil {
			return nil, err
		}
	}

	tableName := dataTableName(p.TableName)
	if p.IndexName != "" {
		tableName = indexTableName(p.TableName, p.IndexName)
	}

	argIdx := 1
	var args []any
	where := "TRUE"

	if p.ExclusiveStartPK != "" {
		where = fmt.Sprintf("(pk, sk) > ($%d, $%d)", argIdx, argIdx+1)
		args = append(args, p.ExclusiveStartPK, p.ExclusiveStartSK)
	}

	if p.SelectCount && p.FilterSQL == "" {
		q := fmt.Sprintf(`SELECT COUNT(*) FROM %q WHERE %s`, tableName, where)
		var count int
		if err := s.pool.QueryRow(ctx, q, args...).Scan(&count); err != nil {
			return nil, fmt.Errorf("scan count: %w", err)
		}
		return &QueryResult{Count: count, ScannedCount: count}, nil
	}

	limitClause := ""
	if p.Limit > 0 {
		limitClause = fmt.Sprintf(" LIMIT %d", p.Limit)
	}

	q := fmt.Sprintf(`SELECT pk, sk, attributes FROM %q WHERE %s ORDER BY pk, sk%s`,
		tableName, where, limitClause)

	return s.executeItemQuery(ctx, q, args, p.Limit, p.SelectCount)
}

func (s *Store) executeItemQuery(ctx context.Context, query string, args []any, limit int, selectCount bool) (*QueryResult, error) {
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("execute query: %w", err)
	}
	defer rows.Close()

	initialCap := 16
	if limit > 0 && limit < initialCap {
		initialCap = limit
	}
	allItems := make([]Item, 0, initialCap)
	var lastPK, lastSK string
	scannedCount := 0

	for rows.Next() {
		var pk, sk string
		var attrsJSON []byte
		if err := rows.Scan(&pk, &sk, &attrsJSON); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}
		scannedCount++
		lastPK = pk
		lastSK = sk

		var item Item
		if err := json.Unmarshal(attrsJSON, &item); err != nil {
			return nil, fmt.Errorf("unmarshal item: %w", err)
		}
		allItems = append(allItems, item)
	}

	result := &QueryResult{
		ScannedCount: scannedCount,
		Items:        allItems,
		Count:        len(allItems),
	}

	if selectCount {
		result.Items = nil
	}

	if limit > 0 && scannedCount >= limit {
		result.LastEvaluatedPK = lastPK
		result.LastEvaluatedSK = lastSK
	}

	return result, nil
}

// GetTableMetaTx retrieves metadata within a transaction.
func (s *Store) GetTableMetaTx(ctx context.Context, tx pgx.Tx, tableName string) (*TableMeta, error) {
	if meta, ok := s.cache.Get(tableName); ok {
		return meta, nil
	}

	meta, err := s.getTableMetaFromDB(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}

	s.cache.Set(tableName, meta)
	return meta, nil
}

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
