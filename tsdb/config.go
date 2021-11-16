package tsdb

import (
	"errors"
	"fmt"
	"time"
)

const (
	// DefaultEngine is the default engine for new shards
	DefaultEngine = "tsm1"

	// DefaultIndex is the default index for new shards
	DefaultIndex = InmemIndexName

	// tsdb/engine/wal configuration options

	// Default settings for TSM

	// DefaultCacheMaxMemorySize is the maximum size a shard's cache can
	// reach before it starts rejecting writes.
	DefaultCacheMaxMemorySize = 1024 * 1024 * 1024 // 1GB

	// DefaultCacheSnapshotMemorySize is the size at which the engine will
	// snapshot the cache and write it to a TSM file, freeing up memory
	DefaultCacheSnapshotMemorySize = 25 * 1024 * 1024 // 25MB

	// DefaultCacheSnapshotWriteColdDuration is the length of time at which
	// the engine will snapshot the cache and write it to a new TSM file if
	// the shard hasn't received writes or deletes
	DefaultCacheSnapshotWriteColdDuration = time.Duration(10 * time.Minute)

	// DefaultCompactFullWriteColdDuration is the duration at which the engine
	// will compact all TSM files in a shard if it hasn't received a write or delete
	DefaultCompactFullWriteColdDuration = time.Duration(4 * time.Hour)

	// DefaultMaxPointsPerBlock is the maximum number of points in an encoded
	// block in a TSM file
	DefaultMaxPointsPerBlock = 1000

	// DefaultMaxSeriesPerDatabase is the maximum number of series a node can hold per database.
	// This limit only applies to the "inmem" index.
	DefaultMaxSeriesPerDatabase = 1000000

	// DefaultMaxValuesPerTag is the maximum number of values a tag can have within a metric.
	DefaultMaxValuesPerTag = 100000

	// DefaultMaxConcurrentCompactions is the maximum number of concurrent full and level compactions
	// that can run at one time.  A value of 0 results in 50% of runtime.GOMAXPROCS(0) used at runtime.
	DefaultMaxConcurrentCompactions = 0

	// DefaultMaxIndexLogFileSize is the default threshold, in bytes, when an index
	// write-ahead log file will compact into an index file.
	DefaultMaxIndexLogFileSize = 1 * 1024 * 1024 // 1MB

	// DefaultSeriesIDSetCacheSize is the default number of series ID sets to cache in the TSI index.
	DefaultSeriesIDSetCacheSize = 100
)

// Config holds the configuration for the tsbd package.
type Config struct {
	Dir    string
	Engine string
	Index  string

	// General WAL configuration options
	WALDir string

	// WALFsyncDelay is the amount of time that a write will wait before fsyncing.  A duration
	// greater than 0 can be used to batch up multiple fsync calls.  This is useful for slower
	// disks or when WAL write contention is seen.  A value of 0 fsyncs every write to the WAL.
	WALFsyncDelay time.Duration

	// Enables unicode validation on series keys on write.
	ValidateKeys bool

	// Query logging
	QueryLogEnabled bool

	// Compaction options for tsm1 (descriptions above with defaults)
	CacheMaxMemorySize             uint64
	CacheSnapshotMemorySize        uint64
	CacheSnapshotWriteColdDuration time.Duration
	CompactFullWriteColdDuration   time.Duration

	// Limits

	// MaxSeriesPerDatabase is the maximum number of series a node can hold per database.
	// When this limit is exceeded, writes return a 'max series per database exceeded' error.
	// A value of 0 disables the limit. This limit only applies when using the "inmem" index.
	MaxSeriesPerDatabase int

	// MaxValuesPerTag is the maximum number of tag values a single tag key can have within
	// a metric.  When the limit is execeeded, writes return an error.
	// A value of 0 disables the limit.
	MaxValuesPerTag int

	// MaxConcurrentCompactions is the maximum number of concurrent level and full compactions
	// that can be running at one time across all shards.  Compactions scheduled to run when the
	// limit is reached are blocked until a running compaction completes.  Snapshot compactions are
	// not affected by this limit.  A value of 0 limits compactions to runtime.GOMAXPROCS(0).
	MaxConcurrentCompactions int

	// MaxIndexLogFileSize is the threshold, in bytes, when an index write-ahead log file will
	// compact into an index file. Lower sizes will cause log files to be compacted more quickly
	// and result in lower heap usage at the expense of write throughput. Higher sizes will
	// be compacted less frequently, store more series in-memory, and provide higher write throughput.
	MaxIndexLogFileSize uint64

	// SeriesIDSetCacheSize is the number items that can be cached within the TSI index. TSI caching can help
	// with query performance when the same tag key/value predicates are commonly used on queries.
	// Setting series-id-set-cache-size to 0 disables the cache.
	SeriesIDSetCacheSize int

	TraceLoggingEnabled bool

	// TSMWillNeed controls whether we hint to the kernel that we intend to
	// page in mmap'd sections of TSM files. This setting defaults to off, as it has
	// been found to be problematic in some cases. It may help users who have
	// slow disks.
	TSMWillNeed bool
}

// NewConfig returns the default configuration for tsdb.
func NewConfig() Config {
	return Config{
		Engine: DefaultEngine,
		Index:  DefaultIndex,

		QueryLogEnabled: true,

		CacheMaxMemorySize:             DefaultCacheMaxMemorySize,
		CacheSnapshotMemorySize:        DefaultCacheSnapshotMemorySize,
		CacheSnapshotWriteColdDuration: 10 * time.Second,
		CompactFullWriteColdDuration:   DefaultCompactFullWriteColdDuration,

		MaxSeriesPerDatabase:     DefaultMaxSeriesPerDatabase,
		MaxValuesPerTag:          DefaultMaxValuesPerTag,
		MaxConcurrentCompactions: DefaultMaxConcurrentCompactions,

		MaxIndexLogFileSize:  DefaultMaxIndexLogFileSize,
		SeriesIDSetCacheSize: DefaultSeriesIDSetCacheSize,

		TraceLoggingEnabled: false,
		TSMWillNeed:         false,
	}
}

// Validate validates the configuration hold by c.
func (c *Config) Validate() error {
	if c.Dir == "" {
		return errors.New("Data.Dir must be specified")
	} else if c.WALDir == "" {
		return errors.New("Data.WALDir must be specified")
	}

	if c.MaxConcurrentCompactions < 0 {
		return errors.New("max-concurrent-compactions must be non-negative")
	}

	if c.SeriesIDSetCacheSize < 0 {
		return errors.New("series-id-set-cache-size must be non-negative")
	}

	valid := false
	for _, e := range RegisteredEngines() {
		if e == c.Engine {
			valid = true
			break
		}
	}
	if !valid {
		return fmt.Errorf("unrecognized engine %s", c.Engine)
	}

	valid = false
	for _, e := range RegisteredIndexes() {
		if e == c.Index {
			valid = true
			break
		}
	}
	if !valid {
		return fmt.Errorf("unrecognized index %s", c.Index)
	}

	return nil
}
