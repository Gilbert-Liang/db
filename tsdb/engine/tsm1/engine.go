// Package tsm1 provides a TSDB in the Time Structured Merge tree format.
package tsm1

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"db/logger"
	"db/model"
	"db/parser/cnosql"
	"db/pkg/bytesutil"
	"db/pkg/estimator"
	"db/pkg/limiter"
	"db/pkg/metrics"
	"db/pkg/radix"
	"db/pkg/tracing"
	"db/query"
	"db/tsdb"
	_ "db/tsdb/index"
	"db/tsdb/index/inmem"
	"db/tsdb/index/tsi1"

	"go.uber.org/zap"
)

//go:generate tmpl -data=@iterator.gen.go.tmpldata iterator.gen.go.tmpl engine.gen.go.tmpl array_cursor.gen.go.tmpl array_cursor_iterator.gen.go.tmpl
//go:generate tmpl -data=@file_store.gen.go.tmpldata file_store.gen.go.tmpl file_store_array.gen.go.tmpl
//go:generate tmpl -data=@encoding.gen.go.tmpldata encoding.gen.go.tmpl
//go:generate tmpl -data=@compact.gen.go.tmpldata compact.gen.go.tmpl
//go:generate tmpl -data=@reader.gen.go.tmpldata reader.gen.go.tmpl

func init() {
	tsdb.RegisterEngine("tsm1", NewEngine)
}

var (
	// Ensure Engine implements the interface.
	_ tsdb.Engine = &Engine{}
	// Static objects to prevent small allocs.
	timeBytes              = []byte("time")
	keyFieldSeparatorBytes = []byte(keyFieldSeparator)
	emptyBytes             = []byte{}
)

var (
	tsmGroup                   = metrics.MustRegisterGroup("tsm1")
	numberOfRefCursorsCounter  = metrics.MustRegisterCounter("cursors_ref", metrics.WithGroup(tsmGroup))
	numberOfAuxCursorsCounter  = metrics.MustRegisterCounter("cursors_aux", metrics.WithGroup(tsmGroup))
	numberOfCondCursorsCounter = metrics.MustRegisterCounter("cursors_cond", metrics.WithGroup(tsmGroup))
	planningTimer              = metrics.MustRegisterTimer("planning_time", metrics.WithGroup(tsmGroup))
)

// NewContextWithMetricsGroup creates a new context with a tsm1 metrics.Group for tracking
// various metrics when accessing TSM data.
func NewContextWithMetricsGroup(ctx context.Context) context.Context {
	group := metrics.NewGroup(tsmGroup)
	return metrics.NewContextWithGroup(ctx, group)
}

// MetricsGroupFromContext returns the tsm1 metrics.Group associated with the context
// or nil if no group has been assigned.
func MetricsGroupFromContext(ctx context.Context) *metrics.Group {
	return metrics.GroupFromContext(ctx)
}

const (
	// keyFieldSeparator separates the series key from the field name in the composite key
	// that identifies a specific field in series
	keyFieldSeparator = "#!~#"

	// deleteFlushThreshold is the size in bytes of a batch of series keys to delete.
	deleteFlushThreshold = 50 * 1024 * 1024
)

// Engine represents a storage engine with compressed blocks.
type Engine struct {
	mu sync.RWMutex

	index tsdb.Index

	// The following group of fields is used to track the state of level compactions within the
	// Engine. The WaitGroup is used to monitor the compaction goroutines, the 'done' channel is
	// used to signal those goroutines to shutdown. Every request to disable level compactions will
	// call 'Wait' on 'wg', with the first goroutine to arrive (levelWorkers == 0 while holding the
	// lock) will close the done channel and re-assign 'nil' to the variable. Re-enabling will
	// decrease 'levelWorkers', and when it decreases to zero, level compactions will be started
	// back up again.

	wg           *sync.WaitGroup // waitgroup for active level compaction goroutines
	done         chan struct{}   // channel to signal level compactions to stop
	levelWorkers int             // Number of "workers" that expect compactions to be in a disabled state

	snapDone chan struct{}   // channel to signal snapshot compactions to stop
	snapWG   *sync.WaitGroup // waitgroup for running snapshot compactions

	id           uint64
	path         string
	sfile        *tsdb.SeriesFile
	logger       *zap.Logger // Logger to be used for important messages
	traceLogger  *zap.Logger // Logger to be used when trace-logging is on.
	traceLogging bool

	fieldset *tsdb.MetricFieldSet

	Cache     *Cache
	Compactor *Compactor
	FileStore *FileStore

	MaxPointsPerBlock int

	// CacheFlushMemorySizeThreshold specifies the minimum size threshold for
	// the cache when the engine should write a snapshot to a TSM file
	CacheFlushMemorySizeThreshold uint64

	// CacheFlushWriteColdDuration specifies the length of time after which if
	// no writes have been committed to the WAL, the engine will write
	// a snapshot of the cache to a TSM file
	CacheFlushWriteColdDuration time.Duration

	// Invoked when creating a backup file "as new".
	formatFileName FormatFileNameFunc

	// Controls whether to enabled compactions when the engine is open
	enableCompactionsOnOpen bool

	// Limiter for concurrent compactions.
	compactionLimiter limiter.Fixed

	// provides access to the total set of series IDs
	seriesIDSets tsdb.SeriesIDSets

	// seriesTypeMap maps a series key to field type
	seriesTypeMap *radix.Tree

	// muDigest ensures only one goroutine can generate a digest at a time.
	muDigest sync.RWMutex
}

// NewEngine returns a new instance of Engine.
func NewEngine(id uint64, idx tsdb.Index, path string, walPath string, sfile *tsdb.SeriesFile, opt tsdb.EngineOptions) tsdb.Engine {
	fs := NewFileStore(path)
	fs.openLimiter = opt.OpenLimiter
	fs.tsmMMAPWillNeed = opt.Config.TSMWillNeed

	cache := NewCache(opt.Config.CacheMaxMemorySize)

	c := NewCompactor()
	c.Dir = path
	c.FileStore = fs
	c.RateLimit = opt.CompactionThroughputLimiter

	logger := zap.NewNop()
	e := &Engine{
		id:           id,
		path:         path,
		index:        idx,
		sfile:        sfile,
		logger:       logger,
		traceLogger:  logger,
		traceLogging: opt.Config.TraceLoggingEnabled,

		Cache: cache,

		FileStore: fs,
		Compactor: c,

		CacheFlushMemorySizeThreshold: opt.Config.CacheSnapshotMemorySize,
		CacheFlushWriteColdDuration:   opt.Config.CacheSnapshotWriteColdDuration,
		enableCompactionsOnOpen:       true,
		formatFileName:                DefaultFormatFileName,
		compactionLimiter:             opt.CompactionLimiter,
		seriesIDSets:                  opt.SeriesIDSets,
	}

	// Feature flag to enable per-series type checking, by default this is off and
	// e.seriesTypeMap will be nil.
	if os.Getenv("CNOSDB_SERIES_TYPE_CHECK_ENABLED") != "" {
		e.seriesTypeMap = radix.New()
	}

	if e.traceLogging {
		fs.enableTraceLogging(true)
	}

	return e
}

func (e *Engine) WithFormatFileNameFunc(formatFileNameFunc FormatFileNameFunc) {
	e.Compactor.WithFormatFileNameFunc(formatFileNameFunc)
	e.formatFileName = formatFileNameFunc
}

func (e *Engine) WithParseFileNameFunc(parseFileNameFunc ParseFileNameFunc) {
	e.FileStore.WithParseFileNameFunc(parseFileNameFunc)
	e.Compactor.WithParseFileNameFunc(parseFileNameFunc)
}

// SetEnabled sets whether the engine is enabled.
func (e *Engine) SetEnabled(enabled bool) {
	e.enableCompactionsOnOpen = enabled
	e.SetCompactionsEnabled(enabled)
}

// SetCompactionsEnabled enables compactions on the engine.  When disabled
// all running compactions are aborted and new compactions stop running.
func (e *Engine) SetCompactionsEnabled(enabled bool) {
	if enabled {
		e.enableSnapshotCompactions()
	} else {
		e.disableSnapshotCompactions()
	}
}

func (e *Engine) enableSnapshotCompactions() {
	// Check if already enabled under read lock
	e.mu.RLock()
	if e.snapDone != nil {
		e.mu.RUnlock()
		return
	}
	e.mu.RUnlock()

	// Check again under write lock
	e.mu.Lock()
	if e.snapDone != nil {
		e.mu.Unlock()
		return
	}

	e.Compactor.EnableSnapshots()
	e.snapDone = make(chan struct{})
	wg := new(sync.WaitGroup)
	wg.Add(1)
	e.snapWG = wg
	e.mu.Unlock()

	go func() { defer wg.Done(); e.compactCache() }()
}

func (e *Engine) disableSnapshotCompactions() {
	e.mu.Lock()
	if e.snapDone == nil {
		e.mu.Unlock()
		return
	}

	// We may be in the process of stopping snapshots.  See if the channel
	// was closed.
	select {
	case <-e.snapDone:
		e.mu.Unlock()
		return
	default:
	}

	// first one here, disable and wait for completion
	close(e.snapDone)
	e.Compactor.DisableSnapshots()
	wg := e.snapWG
	e.mu.Unlock()

	// Wait for the snapshot goroutine to exit.
	wg.Wait()

	// Signal that the goroutines are exit and everything is stopped by setting
	// snapDone to nil.
	e.mu.Lock()
	e.snapDone = nil
	e.mu.Unlock()

	// If the cache is empty, free up its resources as well.
	if e.Cache.Size() == 0 {
		e.Cache.Free()
	}
}

// ScheduleFullCompaction will force the engine to fully compact all data stored.
// This will cancel and running compactions and snapshot any data in the cache to
// TSM files.  This is an expensive operation.
func (e *Engine) ScheduleFullCompaction() error {
	// Snapshot any data in the cache
	if err := e.WriteSnapshot(); err != nil {
		return err
	}

	return nil
}

// Path returns the path the engine was opened with.
func (e *Engine) Path() string { return e.path }

func (e *Engine) SetFieldName(metric []byte, name string) {
	e.index.SetFieldName(metric, name)
}

func (e *Engine) MetricExists(name []byte) (bool, error) {
	return e.index.MetricExists(name)
}

func (e *Engine) MetricNamesByRegex(re *regexp.Regexp) ([][]byte, error) {
	return e.index.MetricNamesByRegex(re)
}

// MetricFieldSet returns the metric field set.
func (e *Engine) MetricFieldSet() *tsdb.MetricFieldSet {
	return e.fieldset
}

// MetricFields returns the metric fields for a metric.
func (e *Engine) MetricFields(metric []byte) *tsdb.MetricFields {
	return e.fieldset.CreateFieldsIfNotExists(metric)
}

func (e *Engine) HasTagKey(name, key []byte) (bool, error) {
	return e.index.HasTagKey(name, key)
}

func (e *Engine) MetricTagKeysByExpr(name []byte, expr cnosql.Expr) (map[string]struct{}, error) {
	return e.index.MetricTagKeysByExpr(name, expr)
}

func (e *Engine) TagKeyCardinality(name, key []byte) int {
	return e.index.TagKeyCardinality(name, key)
}

// SeriesN returns the unique number of series in the index.
func (e *Engine) SeriesN() int64 {
	return e.index.SeriesN()
}

// MetricsSketches returns sketches that describe the cardinality of the
// metrics in this shard and metrics that were in this shard, but have
// been tombstoned.
func (e *Engine) MetricsSketches() (estimator.Sketch, estimator.Sketch, error) {
	return e.index.MetricsSketches()
}

// SeriesSketches returns sketches that describe the cardinality of the
// series in this shard and series that were in this shard, but have
// been tombstoned.
func (e *Engine) SeriesSketches() (estimator.Sketch, estimator.Sketch, error) {
	return e.index.SeriesSketches()
}

// LastModified returns the time when this shard was last modified.
func (e *Engine) LastModified() time.Time {
	fsTime := e.FileStore.LastModified()

	return fsTime
}

// DiskSize returns the total size in bytes of all TSM and WAL segments on disk.
func (e *Engine) DiskSize() int64 {
	var walDiskSizeBytes int64

	return e.FileStore.DiskSizeBytes() + walDiskSizeBytes
}

// Open opens and initializes the engine.
func (e *Engine) Open() error {
	if err := os.MkdirAll(e.path, 0777); err != nil {
		return err
	}

	if err := e.cleanup(); err != nil {
		return err
	}

	fields, err := tsdb.NewMetricFieldSet(filepath.Join(e.path, "fields.idx"))
	if err != nil {
		e.logger.Warn(fmt.Sprintf("error opening fields.idx: %v.  Rebuilding.", err))
	}

	e.mu.Lock()
	e.fieldset = fields
	e.mu.Unlock()

	e.index.SetFieldSet(fields)

	if err := e.FileStore.Open(); err != nil {
		return err
	}

	e.Compactor.Open()

	if e.enableCompactionsOnOpen {
		e.SetCompactionsEnabled(true)
	}

	return nil
}

// Close closes the engine. Subsequent calls to Close are a nop.
func (e *Engine) Close() error {
	e.SetCompactionsEnabled(false)

	// Lock now and close everything else down.
	e.mu.Lock()
	defer e.mu.Unlock()
	e.done = nil // Ensures that the channel will not be closed again.

	if err := e.FileStore.Close(); err != nil {
		return err
	}

	return nil
}

// WithLogger sets the logger for the engine.
func (e *Engine) WithLogger(log *zap.Logger) {
	e.logger = log.With(zap.String("engine", "tsm1"))

	if e.traceLogging {
		e.traceLogger = e.logger
	}

	e.FileStore.WithLogger(e.logger)
}

// LoadMetadataIndex loads the shard metadata into memory.
//
// Note, it not safe to call LoadMetadataIndex concurrently. LoadMetadataIndex
// should only be called when initialising a new Engine.
func (e *Engine) LoadMetadataIndex(shardID uint64, index tsdb.Index) error {
	now := time.Now()

	// Save reference to index for iterator creation.
	e.index = index

	// If we have the cached fields index on disk and we're using TSI, we
	// can skip scanning all the TSM files.
	if e.index.Type() != inmem.IndexName && !e.fieldset.IsEmpty() {
		return nil
	}

	keys := make([][]byte, 0, 10000)
	fieldTypes := make([]cnosql.DataType, 0, 10000)

	if err := e.FileStore.WalkKeys(nil, func(key []byte, typ byte) error {
		fieldType := BlockTypeToCnosQLDataType(typ)
		if fieldType == cnosql.Unknown {
			return fmt.Errorf("unknown block type: %v", typ)
		}

		keys = append(keys, key)
		fieldTypes = append(fieldTypes, fieldType)
		if len(keys) == cap(keys) {
			// Send batch of keys to the index.
			if err := e.addToIndexFromKey(keys, fieldTypes); err != nil {
				return err
			}

			// Reset buffers.
			keys, fieldTypes = keys[:0], fieldTypes[:0]
		}

		return nil
	}); err != nil {
		return err
	}

	if len(keys) > 0 {
		// Add remaining partial batch from FileStore.
		if err := e.addToIndexFromKey(keys, fieldTypes); err != nil {
			return err
		}
		keys, fieldTypes = keys[:0], fieldTypes[:0]
	}

	// load metadata from the Cache
	if err := e.Cache.ApplyEntryFn(func(key []byte, entry *entry) error {
		fieldType, err := entry.values.CnosQLType()
		if err != nil {
			e.logger.Info("Error getting the data type of values for key", zap.ByteString("key", key), zap.Error(err))
		}

		keys = append(keys, key)
		fieldTypes = append(fieldTypes, fieldType)
		if len(keys) == cap(keys) {
			// Send batch of keys to the index.
			if err := e.addToIndexFromKey(keys, fieldTypes); err != nil {
				return err
			}

			// Reset buffers.
			keys, fieldTypes = keys[:0], fieldTypes[:0]
		}
		return nil
	}); err != nil {
		return err
	}

	if len(keys) > 0 {
		// Add remaining partial batch from FileStore.
		if err := e.addToIndexFromKey(keys, fieldTypes); err != nil {
			return err
		}
	}

	// Save the field set index so we don't have to rebuild it next time
	if err := e.fieldset.Save(); err != nil {
		return err
	}

	e.traceLogger.Info("Meta data index for shard loaded", zap.Uint64("id", shardID), zap.Duration("duration", time.Since(now)))
	return nil
}

// IsIdle returns true if the cache is empty, there are no running compactions and the
// shard is fully compacted.
func (e *Engine) IsIdle() bool {
	cacheEmpty := e.Cache.Size() == 0

	return cacheEmpty
}

// Free releases any resources held by the engine to free up memory or CPU.
func (e *Engine) Free() error {
	e.Cache.Free()
	return e.FileStore.Free()
}

// addToIndexFromKey will pull the metric names, series keys, and field
// names from composite keys, and add them to the database index and metric
// fields.
func (e *Engine) addToIndexFromKey(keys [][]byte, fieldTypes []cnosql.DataType) error {
	var field []byte
	names := make([][]byte, 0, len(keys))
	tags := make([]model.Tags, 0, len(keys))

	for i := 0; i < len(keys); i++ {
		// Replace tsm key format with index key format.
		keys[i], field = SeriesAndFieldFromCompositeKey(keys[i])
		name := model.ParseName(keys[i])
		mf := e.fieldset.CreateFieldsIfNotExists(name)
		if err := mf.CreateFieldIfNotExists(field, fieldTypes[i]); err != nil {
			return err
		}

		names = append(names, name)
		tags = append(tags, model.ParseTags(keys[i]))
	}

	// Build in-memory index, if necessary.
	if e.index.Type() == inmem.IndexName {
		if err := e.index.InitializeSeries(keys, names, tags); err != nil {
			return err
		}
	} else {
		if err := e.index.CreateSeriesListIfNotExists(keys, names, tags); err != nil {
			return err
		}
	}

	return nil
}

// WritePoints writes metadata and point data into the engine.
// It returns an error if new points are added to an existing key.
func (e *Engine) WritePoints(points []model.Point) error {
	values := make(map[string][]Value, len(points))
	var (
		keyBuf    []byte
		baseLen   int
		seriesErr error
	)

	for _, p := range points {
		keyBuf = append(keyBuf[:0], p.Key()...)
		keyBuf = append(keyBuf, keyFieldSeparator...)
		baseLen = len(keyBuf)
		iter := p.FieldIterator()
		t := p.Time().UnixNano()
		for iter.Next() {
			// Skip fields name "time", they are illegal
			if bytes.Equal(iter.FieldKey(), timeBytes) {
				continue
			}

			keyBuf = append(keyBuf[:baseLen], iter.FieldKey()...)

			if e.seriesTypeMap != nil {
				// Fast-path check to see if the field for the series already exists.
				if v, ok := e.seriesTypeMap.Get(keyBuf); !ok {
					if typ, err := e.Type(keyBuf); err != nil {
						// Field type is unknown, we can try to add it.
					} else if typ != iter.Type() {
						// Existing type is different from what was passed in, we need to drop
						// this write and refresh the series type map.
						seriesErr = tsdb.ErrFieldTypeConflict
						e.seriesTypeMap.Insert(keyBuf, int(typ))
						continue
					}

					// Doesn't exist, so try to insert
					vv, ok := e.seriesTypeMap.Insert(keyBuf, int(iter.Type()))

					// We didn't insert and the type that exists isn't what we tried to insert, so
					// we have a conflict and must drop this field/series.
					if !ok || vv != int(iter.Type()) {
						seriesErr = tsdb.ErrFieldTypeConflict
						continue
					}
				} else if v != int(iter.Type()) {
					// The series already exists, but with a different type.  This is also a type conflict
					// and we need to drop this field/series.
					seriesErr = tsdb.ErrFieldTypeConflict
					continue
				}
			}

			var v Value
			switch iter.Type() {
			case model.Float:
				fv, err := iter.FloatValue()
				if err != nil {
					return err
				}
				v = NewFloatValue(t, fv)
			case model.Integer:
				iv, err := iter.IntegerValue()
				if err != nil {
					return err
				}
				v = NewIntegerValue(t, iv)
			case model.Unsigned:
				iv, err := iter.UnsignedValue()
				if err != nil {
					return err
				}
				v = NewUnsignedValue(t, iv)
			case model.String:
				v = NewStringValue(t, iter.StringValue())
			case model.Boolean:
				bv, err := iter.BooleanValue()
				if err != nil {
					return err
				}
				v = NewBooleanValue(t, bv)
			default:
				return fmt.Errorf("unknown field type for %s: %s", string(iter.FieldKey()), p.String())
			}
			values[string(keyBuf)] = append(values[string(keyBuf)], v)
		}
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	// first try to write to the cache
	if err := e.Cache.WriteMulti(values); err != nil {
		return err
	}

	return seriesErr
}

// DeleteSeriesRange removes the values between min and max (inclusive) from all series
func (e *Engine) DeleteSeriesRange(itr tsdb.SeriesIterator, min, max int64) error {
	return e.DeleteSeriesRangeWithPredicate(itr, func(name []byte, tags model.Tags) (int64, int64, bool) {
		return min, max, true
	})
}

// DeleteSeriesRangeWithPredicate removes the values between min and max (inclusive) from all series
// for which predicate() returns true. If predicate() is nil, then all values in range are removed.
func (e *Engine) DeleteSeriesRangeWithPredicate(itr tsdb.SeriesIterator, predicate func(name []byte, tags model.Tags) (int64, int64, bool)) error {
	var disableOnce bool

	// Ensure that the index does not compact away the metric or series we're
	// going to delete before we're done with them.
	if tsiIndex, ok := e.index.(*tsi1.Index); ok {
		tsiIndex.DisableCompactions()
		defer tsiIndex.EnableCompactions()
		tsiIndex.Wait()

		fs, err := tsiIndex.RetainFileSet()
		if err != nil {
			return err
		}
		defer fs.Release()
	}

	var (
		sz       int
		min, max int64 = math.MinInt64, math.MaxInt64

		// Indicator that the min/max time for the current batch has changed and
		// we need to flush the current batch before appending to it.
		flushBatch bool
	)

	// These are reversed from min/max to ensure they are different the first time through.
	newMin, newMax := int64(math.MaxInt64), int64(math.MinInt64)

	// There is no predicate, so setup newMin/newMax to delete the full time range.
	if predicate == nil {
		newMin = min
		newMax = max
	}

	batch := make([][]byte, 0, 10000)
	for {
		elem, err := itr.Next()
		if err != nil {
			return err
		} else if elem == nil {
			break
		}

		// See if the series should be deleted and if so, what range of time.
		if predicate != nil {
			var shouldDelete bool
			newMin, newMax, shouldDelete = predicate(elem.Name(), elem.Tags())
			if !shouldDelete {
				continue
			}

			// If the min/max happens to change for the batch, we need to flush
			// the current batch and start a new one.
			flushBatch = (min != newMin || max != newMax) && len(batch) > 0
		}

		if elem.Expr() != nil {
			if v, ok := elem.Expr().(*cnosql.BooleanLiteral); !ok || !v.Val {
				return errors.New("fields not supported in WHERE clause during deletion")
			}
		}

		if !disableOnce {
			// Disable and abort running compactions so that tombstones added existing tsm
			// files don't get removed.  This would cause deleted metrics/series to
			// re-appear once the compaction completed.  We only disable the level compactions
			// so that snapshotting does not stop while writing out tombstones.  If it is stopped,
			// and writing tombstones takes a long time, writes can get rejected due to the cache
			// filling up.
			e.sfile.DisableCompactions()
			defer e.sfile.EnableCompactions()
			e.sfile.Wait()

			disableOnce = true
		}

		if sz >= deleteFlushThreshold || flushBatch {
			// Delete all matching batch.
			if err := e.deleteSeriesRange(batch, min, max); err != nil {
				return err
			}
			batch = batch[:0]
			sz = 0
			flushBatch = false
		}

		// Use the new min/max time for the next iteration
		min = newMin
		max = newMax

		key := model.MakeKey(elem.Name(), elem.Tags())
		sz += len(key)
		batch = append(batch, key)
	}

	if len(batch) > 0 {
		// Delete all matching batch.
		if err := e.deleteSeriesRange(batch, min, max); err != nil {
			return err
		}
	}

	e.index.Rebuild()
	return nil
}

// deleteSeriesRange removes the values between min and max (inclusive) from all series.  This
// does not update the index or disable compactions.  This should mainly be called by DeleteSeriesRange
// and not directly.
func (e *Engine) deleteSeriesRange(seriesKeys [][]byte, min, max int64) error {
	if len(seriesKeys) == 0 {
		return nil
	}

	// Ensure keys are sorted since lower layers require them to be.
	if !bytesutil.IsSorted(seriesKeys) {
		bytesutil.Sort(seriesKeys)
	}

	// Min and max time in the engine are slightly different from the query language values.
	if min == cnosql.MinTime {
		min = math.MinInt64
	}
	if max == cnosql.MaxTime {
		max = math.MaxInt64
	}

	// Run the delete on each TSM file in parallel
	if err := e.FileStore.Apply(func(r TSMFile) error {
		// See if this TSM file contains the keys and time range
		minKey, maxKey := seriesKeys[0], seriesKeys[len(seriesKeys)-1]
		tsmMin, tsmMax := r.KeyRange()

		tsmMin, _ = SeriesAndFieldFromCompositeKey(tsmMin)
		tsmMax, _ = SeriesAndFieldFromCompositeKey(tsmMax)

		overlaps := bytes.Compare(tsmMin, maxKey) <= 0 && bytes.Compare(tsmMax, minKey) >= 0
		if !overlaps || !r.OverlapsTimeRange(min, max) {
			return nil
		}

		// Delete each key we find in the file.  We seek to the min key and walk from there.
		batch := r.BatchDelete()
		n := r.KeyCount()
		var j int
		for i := r.Seek(minKey); i < n; i++ {
			indexKey, _ := r.KeyAt(i)
			seriesKey, _ := SeriesAndFieldFromCompositeKey(indexKey)

			for j < len(seriesKeys) && bytes.Compare(seriesKeys[j], seriesKey) < 0 {
				j++
			}

			if j >= len(seriesKeys) {
				break
			}
			if bytes.Equal(seriesKeys[j], seriesKey) {
				if err := batch.DeleteRange([][]byte{indexKey}, min, max); err != nil {
					batch.Rollback()
					return err
				}
			}
		}

		return batch.Commit()
	}); err != nil {
		return err
	}

	// find the keys in the cache and remove them
	deleteKeys := make([][]byte, 0, len(seriesKeys))

	// ApplySerialEntryFn cannot return an error in this invocation.
	_ = e.Cache.ApplyEntryFn(func(k []byte, _ *entry) error {
		seriesKey, _ := SeriesAndFieldFromCompositeKey([]byte(k))

		// Cache does not walk keys in sorted order, so search the sorted
		// series we need to delete to see if any of the cache keys match.
		i := bytesutil.SearchBytes(seriesKeys, seriesKey)
		if i < len(seriesKeys) && bytes.Equal(seriesKey, seriesKeys[i]) {
			// k is the metric + tags + sep + field
			deleteKeys = append(deleteKeys, k)
		}
		return nil
	})

	// Sort the series keys because ApplyEntryFn iterates over the keys randomly.
	bytesutil.Sort(deleteKeys)

	e.Cache.DeleteRange(deleteKeys, min, max)

	// The series are deleted on disk, but the index may still say they exist.
	// Depending on the the min,max time passed in, the series may or not actually
	// exists now.  To reconcile the index, we walk the series keys that still exists
	// on disk and cross out any keys that match the passed in series.  Any series
	// left in the slice at the end do not exist and can be deleted from the index.
	// Note: this is inherently racy if writes are occurring to the same metric/series are
	// being removed.  A write could occur and exist in the cache at this point, but we
	// would delete it from the index.
	minKey := seriesKeys[0]

	// Apply runs this func concurrently.  The seriesKeys slice is mutated concurrently
	// by different goroutines setting positions to nil.
	if err := e.FileStore.Apply(func(r TSMFile) error {
		n := r.KeyCount()
		var j int

		// Start from the min deleted key that exists in this file.
		for i := r.Seek(minKey); i < n; i++ {
			if j >= len(seriesKeys) {
				return nil
			}

			indexKey, _ := r.KeyAt(i)
			seriesKey, _ := SeriesAndFieldFromCompositeKey(indexKey)

			// Skip over any deleted keys that are less than our tsm key
			cmp := bytes.Compare(seriesKeys[j], seriesKey)
			for j < len(seriesKeys) && cmp < 0 {
				j++
				if j >= len(seriesKeys) {
					return nil
				}
				cmp = bytes.Compare(seriesKeys[j], seriesKey)
			}

			// We've found a matching key, cross it out so we do not remove it from the index.
			if j < len(seriesKeys) && cmp == 0 {
				seriesKeys[j] = emptyBytes
				j++
			}
		}
		return nil
	}); err != nil {
		return err
	}

	// Have we deleted all values for the series? If so, we need to remove
	// the series from the index.
	hasDeleted := false
	for _, k := range seriesKeys {
		if len(k) > 0 {
			hasDeleted = true
			break
		}
	}
	if hasDeleted {
		buf := make([]byte, 1024) // For use when accessing series file.
		ids := tsdb.NewSeriesIDSet()
		metrics := make(map[string]struct{}, 1)

		for _, k := range seriesKeys {
			if len(k) == 0 {
				continue // This key was wiped because it shouldn't be removed from index.
			}

			name, tags := model.ParseKeyBytes(k)
			sid := e.sfile.SeriesID(name, tags, buf)
			if sid == 0 {
				continue
			}

			// See if this series was found in the cache earlier
			i := bytesutil.SearchBytes(deleteKeys, k)

			var hasCacheValues bool
			// If there are multiple fields, they will have the same prefix.  If any field
			// has values, then we can't delete it from the index.
			for i < len(deleteKeys) && bytes.HasPrefix(deleteKeys[i], k) {
				if e.Cache.Values(deleteKeys[i]).Len() > 0 {
					hasCacheValues = true
					break
				}
				i++
			}

			if hasCacheValues {
				continue
			}

			metrics[string(name)] = struct{}{}
			// Remove the series from the local index.
			if err := e.index.DropSeries(sid, k, false); err != nil {
				return err
			}

			// Add the id to the set of delete ids.
			ids.Add(sid)
		}

		fielsetChanged := false
		for k := range metrics {
			if dropped, err := e.index.DropMetricIfSeriesNotExist([]byte(k)); err != nil {
				return err
			} else if dropped {
				if err := e.cleanupMetric([]byte(k)); err != nil {
					return err
				}
				fielsetChanged = true
			}
		}
		if fielsetChanged {
			if err := e.fieldset.Save(); err != nil {
				return err
			}
		}

		// Remove any series IDs for our set that still exist in other shards.
		// We cannot remove these from the series file yet.
		if err := e.seriesIDSets.ForEach(func(s *tsdb.SeriesIDSet) {
			ids = ids.AndNot(s)
		}); err != nil {
			return err
		}

		// Remove the remaining ids from the series file as they no longer exist
		// in any shard.
		var err error
		ids.ForEach(func(id uint64) {
			name, tags := e.sfile.Series(id)
			if err1 := e.sfile.DeleteSeriesID(id); err1 != nil {
				err = err1
				return
			}

			// In the case of the inmem index the series can be removed across
			// the global index (all shards).
			if index, ok := e.index.(*inmem.ShardIndex); ok {
				key := model.MakeKey(name, tags)
				if e := index.Index.DropSeriesGlobal(key); e != nil {
					err = e
				}
			}
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *Engine) cleanupMetric(name []byte) error {
	// A sentinel error message to cause DeleteWithLock to not delete the metric
	abortErr := fmt.Errorf("metrics still exist")

	// Under write lock, delete the metric if we no longer have any data stored for
	// the metric.  If data exists, we can't delete the field set yet as there
	// were writes to the metric while we are deleting it.
	if err := e.fieldset.DeleteWithLock(string(name), func() error {
		encodedName := model.EscapeMetric(name)
		sep := len(encodedName)

		// First scan the cache to see if any series exists for this metric.
		if err := e.Cache.ApplyEntryFn(func(k []byte, _ *entry) error {
			if bytes.HasPrefix(k, encodedName) && (k[sep] == ',' || k[sep] == keyFieldSeparator[0]) {
				return abortErr
			}
			return nil
		}); err != nil {
			return err
		}

		// Check the filestore.
		return e.FileStore.WalkKeys(name, func(k []byte, _ byte) error {
			if bytes.HasPrefix(k, encodedName) && (k[sep] == ',' || k[sep] == keyFieldSeparator[0]) {
				return abortErr
			}
			return nil
		})

	}); err != nil && err != abortErr {
		// Something else failed, return it
		return err
	}

	return nil
}

// DeleteMetric deletes a metric and all related series.
func (e *Engine) DeleteMetric(name []byte) error {
	// Attempt to find the series keys.
	indexSet := tsdb.IndexSet{Indexes: []tsdb.Index{e.index}, SeriesFile: e.sfile}
	itr, err := indexSet.MetricSeriesByExprIterator(name, nil)
	if err != nil {
		return err
	} else if itr == nil {
		return nil
	}
	defer itr.Close()
	return e.DeleteSeriesRange(tsdb.NewSeriesIteratorAdapter(e.sfile, itr), math.MinInt64, math.MaxInt64)
}

// ForEachMetricName iterates over each metric name in the engine.
func (e *Engine) ForEachMetricName(fn func(name []byte) error) error {
	return e.index.ForEachMetricName(fn)
}

func (e *Engine) CreateSeriesListIfNotExists(keys, names [][]byte, tagsSlice []model.Tags) error {
	return e.index.CreateSeriesListIfNotExists(keys, names, tagsSlice)
}

func (e *Engine) CreateSeriesIfNotExists(key, name []byte, tags model.Tags) error {
	return e.index.CreateSeriesIfNotExists(key, name, tags)
}

// WriteTo is not implemented.
func (e *Engine) WriteTo(w io.Writer) (n int64, err error) { panic("not implemented") }

// WriteSnapshot will snapshot the cache and write a new TSM file with its contents, releasing the snapshot when done.
func (e *Engine) WriteSnapshot() (err error) {
	// Lock and grab the cache snapshot along with all the closed WAL
	// filenames associated with the snapshot

	started := time.Now()
	log, logEnd := logger.NewOperation(e.logger, "Cache snapshot", "tsm1_cache_snapshot")
	defer func() {
		elapsed := time.Since(started)

		if err == nil {
			log.Info("Snapshot for path written", zap.String("path", e.path), zap.Duration("duration", elapsed))
		}
		logEnd()
	}()

	closedFiles, snapshot, err := func() (segments []string, snapshot *Cache, err error) {
		e.mu.Lock()
		defer e.mu.Unlock()

		snapshot, err = e.Cache.Snapshot()
		if err != nil {
			return
		}

		return
	}()

	if err != nil {
		return err
	}

	if snapshot.Size() == 0 {
		e.Cache.ClearSnapshot(true)
		return nil
	}

	// The snapshotted cache may have duplicate points and unsorted data.  We need to deduplicate
	// it before writing the snapshot.  This can be very expensive so it's done while we are not
	// holding the engine write lock.
	dedup := time.Now()
	snapshot.Deduplicate()
	e.traceLogger.Info("Snapshot for path deduplicated",
		zap.String("path", e.path),
		zap.Duration("duration", time.Since(dedup)))

	return e.writeSnapshotAndCommit(log, closedFiles, snapshot)
}

// CreateSnapshot will create a temp directory that holds
// temporary hardlinks to the underlying shard files.
func (e *Engine) CreateSnapshot() (string, error) {
	if err := e.WriteSnapshot(); err != nil {
		return "", err
	}

	e.mu.RLock()
	defer e.mu.RUnlock()
	path, err := e.FileStore.CreateSnapshot()
	if err != nil {
		return "", err
	}

	// Generate a snapshot of the index.
	return path, nil
}

// writeSnapshotAndCommit will write the passed cache to a new TSM file and remove the closed SAL segments.
func (e *Engine) writeSnapshotAndCommit(log *zap.Logger, closedFiles []string, snapshot *Cache) (err error) {
	defer func() {
		if err != nil {
			e.Cache.ClearSnapshot(false)
		}
	}()

	// write the new snapshot files
	newFiles, err := e.Compactor.WriteSnapshot(snapshot)
	if err != nil {
		log.Info("Error writing snapshot from compactor", zap.Error(err))
		return err
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	// update the file store with these new files
	if err := e.FileStore.Replace(nil, newFiles); err != nil {
		log.Info("Error adding new TSM files from snapshot. Removing temp files.", zap.Error(err))

		// Remove the new snapshot files. We will try again.
		for _, file := range newFiles {
			if err := os.Remove(file); err != nil {
				log.Info("Unable to remove file", zap.String("path", file), zap.Error(err))
			}
		}
		return err
	}

	// clear the snapshot from the in-memory cache, then the old WAL files
	e.Cache.ClearSnapshot(true)

	return nil
}

// compactCache continually checks if the WAL cache should be written to disk.
func (e *Engine) compactCache() {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		e.mu.RLock()
		quit := e.snapDone
		e.mu.RUnlock()

		select {
		case <-quit:
			return

		case <-t.C:
			if e.ShouldCompactCache(time.Now()) {
				e.traceLogger.Info("Compacting cache", zap.String("path", e.path))
				err := e.WriteSnapshot()
				if err != nil && err != errCompactionsDisabled {
					e.logger.Info("Error writing snapshot", zap.Error(err))
				}
			}
		}
	}
}

// ShouldCompactCache returns true if the Cache is over its flush threshold
// or if the passed in lastWriteTime is older than the write cold threshold.
func (e *Engine) ShouldCompactCache(t time.Time) bool {
	sz := e.Cache.Size()

	if sz == 0 {
		return false
	}

	if sz > e.CacheFlushMemorySizeThreshold {
		return true
	}

	return t.Sub(e.Cache.LastWriteTime()) > e.CacheFlushWriteColdDuration
}

// cleanup removes all temp files and dirs that exist on disk.  This is should only be run at startup to avoid
// removing tmp files that are still in use.
func (e *Engine) cleanup() error {
	allfiles, err := ioutil.ReadDir(e.path)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}

	ext := fmt.Sprintf(".%s", TmpTSMFileExtension)
	for _, f := range allfiles {
		// Check to see if there are any `.tmp` directories that were left over from failed shard snapshots
		if f.IsDir() && strings.HasSuffix(f.Name(), ext) {
			if err := os.RemoveAll(filepath.Join(e.path, f.Name())); err != nil {
				return fmt.Errorf("error removing tmp snapshot directory %q: %s", f.Name(), err)
			}
		}
	}

	return e.cleanupTempTSMFiles()
}

func (e *Engine) cleanupTempTSMFiles() error {
	files, err := filepath.Glob(filepath.Join(e.path, fmt.Sprintf("*.%s", CompactionTempExtension)))
	if err != nil {
		return fmt.Errorf("error getting compaction temp files: %s", err.Error())
	}

	for _, f := range files {
		if err := os.Remove(f); err != nil {
			return fmt.Errorf("error removing temp compaction files: %v", err)
		}
	}
	return nil
}

// KeyCursor returns a KeyCursor for the given key starting at time t.
func (e *Engine) KeyCursor(ctx context.Context, key []byte, t int64, ascending bool) *KeyCursor {
	return e.FileStore.KeyCursor(ctx, key, t, ascending)
}

// CreateIterator returns an iterator for the metric based on opt.
func (e *Engine) CreateIterator(ctx context.Context, metric string, opt query.IteratorOptions) (query.Iterator, error) {
	if span := tracing.SpanFromContext(ctx); span != nil {
		labels := []string{"shard_id", strconv.Itoa(int(e.id)), "metric", metric}
		if opt.Condition != nil {
			labels = append(labels, "cond", opt.Condition.String())
		}

		span = span.StartSpan("create_iterator")
		span.SetLabels(labels...)
		ctx = tracing.NewContextWithSpan(ctx, span)

		group := metrics.NewGroup(tsmGroup)
		ctx = metrics.NewContextWithGroup(ctx, group)
		start := time.Now()

		defer group.GetTimer(planningTimer).UpdateSince(start)
	}

	if call, ok := opt.Expr.(*cnosql.Call); ok {
		if opt.Interval.IsZero() {
			if call.Name == "first" || call.Name == "last" {
				refOpt := opt
				refOpt.Limit = 1
				refOpt.Ascending = call.Name == "first"
				refOpt.Ordered = true
				refOpt.Expr = call.Args[0]

				itrs, err := e.createVarRefIterator(ctx, metric, refOpt)
				if err != nil {
					return nil, err
				}
				return newMergeFinalizerIterator(ctx, itrs, opt, e.logger)
			}
		}

		inputs, err := e.createCallIterator(ctx, metric, call, opt)
		if err != nil {
			return nil, err
		} else if len(inputs) == 0 {
			return nil, nil
		}
		return newMergeFinalizerIterator(ctx, inputs, opt, e.logger)
	}

	itrs, err := e.createVarRefIterator(ctx, metric, opt)
	if err != nil {
		return nil, err
	}
	return newMergeFinalizerIterator(ctx, itrs, opt, e.logger)
}

type indexTagSets interface {
	TagSets(name []byte, options query.IteratorOptions) ([]*query.TagSet, error)
}

func (e *Engine) createCallIterator(ctx context.Context, metric string, call *cnosql.Call, opt query.IteratorOptions) ([]query.Iterator, error) {
	ref, _ := call.Args[0].(*cnosql.VarRef)

	if exists, err := e.index.MetricExists([]byte(metric)); err != nil {
		return nil, err
	} else if !exists {
		return nil, nil
	}

	// Determine tagsets for this metric based on dimensions and filters.
	var (
		tagSets []*query.TagSet
		err     error
	)
	if e.index.Type() == tsdb.InmemIndexName {
		ts := e.index.(indexTagSets)
		tagSets, err = ts.TagSets([]byte(metric), opt)
	} else {
		indexSet := tsdb.IndexSet{Indexes: []tsdb.Index{e.index}, SeriesFile: e.sfile}
		tagSets, err = indexSet.TagSets(e.sfile, []byte(metric), opt)
	}

	if err != nil {
		return nil, err
	}

	// Reverse the tag sets if we are ordering by descending.
	if !opt.Ascending {
		for _, t := range tagSets {
			t.Reverse()
		}
	}

	// Calculate tag sets and apply SLIMIT/SOFFSET.
	tagSets = query.LimitTagSets(tagSets, opt.SLimit, opt.SOffset)

	itrs := make([]query.Iterator, 0, len(tagSets))
	if err := func() error {
		for _, t := range tagSets {
			// Abort if the query was killed
			select {
			case <-opt.InterruptCh:
				query.Iterators(itrs).Close()
				return query.ErrQueryInterrupted
			default:
			}

			inputs, err := e.createTagSetIterators(ctx, ref, metric, t, opt)
			if err != nil {
				return err
			} else if len(inputs) == 0 {
				continue
			}

			// Wrap each series in a call iterator.
			for i, input := range inputs {
				if opt.InterruptCh != nil {
					input = query.NewInterruptIterator(input, opt.InterruptCh)
				}

				itr, err := query.NewCallIterator(input, opt)
				if err != nil {
					query.Iterators(inputs).Close()
					return err
				}
				inputs[i] = itr
			}

			itr := query.NewParallelMergeIterator(inputs, opt, runtime.GOMAXPROCS(0))
			itrs = append(itrs, itr)
		}
		return nil
	}(); err != nil {
		query.Iterators(itrs).Close()
		return nil, err
	}

	return itrs, nil
}

// createVarRefIterator creates an iterator for a variable reference.
func (e *Engine) createVarRefIterator(ctx context.Context, metric string, opt query.IteratorOptions) ([]query.Iterator, error) {
	ref, _ := opt.Expr.(*cnosql.VarRef)

	if exists, err := e.index.MetricExists([]byte(metric)); err != nil {
		return nil, err
	} else if !exists {
		return nil, nil
	}

	var (
		tagSets []*query.TagSet
		err     error
	)
	if e.index.Type() == tsdb.InmemIndexName {
		ts := e.index.(indexTagSets)
		tagSets, err = ts.TagSets([]byte(metric), opt)
	} else {
		indexSet := tsdb.IndexSet{Indexes: []tsdb.Index{e.index}, SeriesFile: e.sfile}
		tagSets, err = indexSet.TagSets(e.sfile, []byte(metric), opt)
	}

	if err != nil {
		return nil, err
	}

	// Reverse the tag sets if we are ordering by descending.
	if !opt.Ascending {
		for _, t := range tagSets {
			t.Reverse()
		}
	}

	// Calculate tag sets and apply SLIMIT/SOFFSET.
	tagSets = query.LimitTagSets(tagSets, opt.SLimit, opt.SOffset)
	itrs := make([]query.Iterator, 0, len(tagSets))
	if err := func() error {
		for _, t := range tagSets {
			inputs, err := e.createTagSetIterators(ctx, ref, metric, t, opt)
			if err != nil {
				return err
			} else if len(inputs) == 0 {
				continue
			}

			// If we have a LIMIT or OFFSET and the grouping of the outer query
			// is different than the current grouping, we need to perform the
			// limit on each of the individual series keys instead to improve
			// performance.
			if (opt.Limit > 0 || opt.Offset > 0) && len(opt.Dimensions) != len(opt.GroupBy) {
				for i, input := range inputs {
					inputs[i] = newLimitIterator(input, opt)
				}
			}

			itr, err := query.Iterators(inputs).Merge(opt)
			if err != nil {
				query.Iterators(inputs).Close()
				return err
			}

			// Apply a limit on the merged iterator.
			if opt.Limit > 0 || opt.Offset > 0 {
				if len(opt.Dimensions) == len(opt.GroupBy) {
					// When the final dimensions and the current grouping are
					// the same, we will only produce one series so we can use
					// the faster limit iterator.
					itr = newLimitIterator(itr, opt)
				} else {
					// When the dimensions are different than the current
					// grouping, we need to account for the possibility there
					// will be multiple series. The limit iterator in the
					// cnosql package handles that scenario.
					itr = query.NewLimitIterator(itr, opt)
				}
			}
			itrs = append(itrs, itr)
		}
		return nil
	}(); err != nil {
		query.Iterators(itrs).Close()
		return nil, err
	}

	return itrs, nil
}

// createTagSetIterators creates a set of iterators for a tagset.
func (e *Engine) createTagSetIterators(ctx context.Context, ref *cnosql.VarRef, name string, t *query.TagSet, opt query.IteratorOptions) ([]query.Iterator, error) {
	// Set parallelism by number of logical cpus.
	parallelism := runtime.GOMAXPROCS(0)
	if parallelism > len(t.SeriesKeys) {
		parallelism = len(t.SeriesKeys)
	}

	// Create series key groupings w/ return error.
	groups := make([]struct {
		keys    []string
		filters []cnosql.Expr
		itrs    []query.Iterator
		err     error
	}, parallelism)

	// Group series keys.
	n := len(t.SeriesKeys) / parallelism
	for i := 0; i < parallelism; i++ {
		group := &groups[i]

		if i < parallelism-1 {
			group.keys = t.SeriesKeys[i*n : (i+1)*n]
			group.filters = t.Filters[i*n : (i+1)*n]
		} else {
			group.keys = t.SeriesKeys[i*n:]
			group.filters = t.Filters[i*n:]
		}
	}

	// Read series groups in parallel.
	var wg sync.WaitGroup
	for i := range groups {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			groups[i].itrs, groups[i].err = e.createTagSetGroupIterators(ctx, ref, name, groups[i].keys, t, groups[i].filters, opt)
		}(i)
	}
	wg.Wait()

	// Determine total number of iterators so we can allocate only once.
	var itrN int
	for _, group := range groups {
		itrN += len(group.itrs)
	}

	// Combine all iterators together and check for errors.
	var err error
	itrs := make([]query.Iterator, 0, itrN)
	for _, group := range groups {
		if group.err != nil {
			err = group.err
		}
		itrs = append(itrs, group.itrs...)
	}

	// If an error occurred, make sure we close all created iterators.
	if err != nil {
		query.Iterators(itrs).Close()
		return nil, err
	}

	return itrs, nil
}

// createTagSetGroupIterators creates a set of iterators for a subset of a tagset's series.
func (e *Engine) createTagSetGroupIterators(ctx context.Context, ref *cnosql.VarRef, name string, seriesKeys []string, t *query.TagSet, filters []cnosql.Expr, opt query.IteratorOptions) ([]query.Iterator, error) {
	itrs := make([]query.Iterator, 0, len(seriesKeys))
	for i, seriesKey := range seriesKeys {
		var conditionFields []cnosql.VarRef
		if filters[i] != nil {
			// Retrieve non-time fields from this series filter and filter out tags.
			conditionFields = cnosql.ExprNames(filters[i])
		}

		itr, err := e.createVarRefSeriesIterator(ctx, ref, name, seriesKey, t, filters[i], conditionFields, opt)
		if err != nil {
			return itrs, err
		} else if itr == nil {
			continue
		}
		itrs = append(itrs, itr)

		// Abort if the query was killed
		select {
		case <-opt.InterruptCh:
			query.Iterators(itrs).Close()
			return nil, query.ErrQueryInterrupted
		default:
		}

		// Enforce series limit at creation time.
		if opt.MaxSeriesN > 0 && len(itrs) > opt.MaxSeriesN {
			query.Iterators(itrs).Close()
			return nil, fmt.Errorf("max-select-series limit exceeded: (%d/%d)", len(itrs), opt.MaxSeriesN)
		}

	}
	return itrs, nil
}

// createVarRefSeriesIterator creates an iterator for a variable reference for a series.
func (e *Engine) createVarRefSeriesIterator(ctx context.Context, ref *cnosql.VarRef, name string, seriesKey string, t *query.TagSet, filter cnosql.Expr, conditionFields []cnosql.VarRef, opt query.IteratorOptions) (query.Iterator, error) {
	_, tfs := model.ParseKey([]byte(seriesKey))
	tags := query.NewTags(tfs.Map())

	// Create options specific for this series.
	itrOpt := opt
	itrOpt.Condition = filter

	var curCounter, auxCounter, condCounter *metrics.Counter
	if col := metrics.GroupFromContext(ctx); col != nil {
		curCounter = col.GetCounter(numberOfRefCursorsCounter)
		auxCounter = col.GetCounter(numberOfAuxCursorsCounter)
		condCounter = col.GetCounter(numberOfCondCursorsCounter)
	}

	// Build main cursor.
	var cur cursor
	if ref != nil {
		cur = e.buildCursor(ctx, name, seriesKey, tfs, ref, opt)
		// If the field doesn't exist then don't build an iterator.
		if cur == nil {
			return nil, nil
		}
		if curCounter != nil {
			curCounter.Add(1)
		}
	}

	// Build auxiliary cursors.
	// Tag values should be returned if the field doesn't exist.
	var aux []cursorAt
	if len(opt.Aux) > 0 {
		aux = make([]cursorAt, len(opt.Aux))
		for i, ref := range opt.Aux {
			// Create cursor from field if a tag wasn't requested.
			if ref.Type != cnosql.Tag {
				cur := e.buildCursor(ctx, name, seriesKey, tfs, &ref, opt)
				if cur != nil {
					if auxCounter != nil {
						auxCounter.Add(1)
					}
					aux[i] = newBufCursor(cur, opt.Ascending)
					continue
				}

				// If a field was requested, use a nil cursor of the requested type.
				switch ref.Type {
				case cnosql.Float, cnosql.AnyField:
					aux[i] = nilFloatLiteralValueCursor
					continue
				case cnosql.Integer:
					aux[i] = nilIntegerLiteralValueCursor
					continue
				case cnosql.Unsigned:
					aux[i] = nilUnsignedLiteralValueCursor
					continue
				case cnosql.String:
					aux[i] = nilStringLiteralValueCursor
					continue
				case cnosql.Boolean:
					aux[i] = nilBooleanLiteralValueCursor
					continue
				}
			}

			// If field doesn't exist, use the tag value.
			if v := tags.Value(ref.Val); v == "" {
				// However, if the tag value is blank then return a null.
				aux[i] = nilStringLiteralValueCursor
			} else {
				aux[i] = &literalValueCursor{value: v}
			}
		}
	}

	// Remove _tagKey condition field.
	// We can't seach on it because we can't join it to _tagValue based on time.
	if varRefSliceContains(conditionFields, "_tagKey") {
		conditionFields = varRefSliceRemove(conditionFields, "_tagKey")

		// Remove _tagKey conditional references from iterator.
		itrOpt.Condition = cnosql.RewriteExpr(cnosql.CloneExpr(itrOpt.Condition), func(expr cnosql.Expr) cnosql.Expr {
			switch expr := expr.(type) {
			case *cnosql.BinaryExpr:
				if ref, ok := expr.LHS.(*cnosql.VarRef); ok && ref.Val == "_tagKey" {
					return &cnosql.BooleanLiteral{Val: true}
				}
				if ref, ok := expr.RHS.(*cnosql.VarRef); ok && ref.Val == "_tagKey" {
					return &cnosql.BooleanLiteral{Val: true}
				}
			}
			return expr
		})
	}

	// Build conditional field cursors.
	// If a conditional field doesn't exist then ignore the series.
	var conds []cursorAt
	if len(conditionFields) > 0 {
		conds = make([]cursorAt, len(conditionFields))
		for i, ref := range conditionFields {
			// Create cursor from field if a tag wasn't requested.
			if ref.Type != cnosql.Tag {
				cur := e.buildCursor(ctx, name, seriesKey, tfs, &ref, opt)
				if cur != nil {
					if condCounter != nil {
						condCounter.Add(1)
					}
					conds[i] = newBufCursor(cur, opt.Ascending)
					continue
				}

				// If a field was requested, use a nil cursor of the requested type.
				switch ref.Type {
				case cnosql.Float, cnosql.AnyField:
					conds[i] = nilFloatLiteralValueCursor
					continue
				case cnosql.Integer:
					conds[i] = nilIntegerLiteralValueCursor
					continue
				case cnosql.Unsigned:
					conds[i] = nilUnsignedLiteralValueCursor
					continue
				case cnosql.String:
					conds[i] = nilStringLiteralValueCursor
					continue
				case cnosql.Boolean:
					conds[i] = nilBooleanLiteralValueCursor
					continue
				}
			}

			// If field doesn't exist, use the tag value.
			if v := tags.Value(ref.Val); v == "" {
				// However, if the tag value is blank then return a null.
				conds[i] = nilStringLiteralValueCursor
			} else {
				conds[i] = &literalValueCursor{value: v}
			}
		}
	}
	condNames := cnosql.VarRefs(conditionFields).Strings()

	// Limit tags to only the dimensions selected.
	dimensions := opt.GetDimensions()
	tags = tags.Subset(dimensions)

	// If it's only auxiliary fields then it doesn't matter what type of iterator we use.
	if ref == nil {
		if opt.StripName {
			name = ""
		}
		return newFloatIterator(name, tags, itrOpt, nil, aux, conds, condNames), nil
	}

	// Remove name if requested.
	if opt.StripName {
		name = ""
	}

	switch cur := cur.(type) {
	case floatCursor:
		return newFloatIterator(name, tags, itrOpt, cur, aux, conds, condNames), nil
	case integerCursor:
		return newIntegerIterator(name, tags, itrOpt, cur, aux, conds, condNames), nil
	case unsignedCursor:
		return newUnsignedIterator(name, tags, itrOpt, cur, aux, conds, condNames), nil
	case stringCursor:
		return newStringIterator(name, tags, itrOpt, cur, aux, conds, condNames), nil
	case booleanCursor:
		return newBooleanIterator(name, tags, itrOpt, cur, aux, conds, condNames), nil
	default:
		panic("unreachable")
	}
}

// buildCursor creates an untyped cursor for a field.
func (e *Engine) buildCursor(ctx context.Context, metric, seriesKey string, tags model.Tags, ref *cnosql.VarRef, opt query.IteratorOptions) cursor {
	// Check if this is a system field cursor.
	switch ref.Val {
	case "_name":
		return &stringSliceCursor{values: []string{metric}}
	case "_tagKey":
		return &stringSliceCursor{values: tags.Keys()}
	case "_tagValue":
		return &stringSliceCursor{values: matchTagValues(tags, opt.Condition)}
	case "_seriesKey":
		return &stringSliceCursor{values: []string{seriesKey}}
	}

	// Look up fields for metric.
	mf := e.fieldset.FieldsByString(metric)
	if mf == nil {
		return nil
	}

	// Check for system field for field keys.
	if ref.Val == "_fieldKey" {
		return &stringSliceCursor{values: mf.FieldKeys()}
	}

	// Find individual field.
	f := mf.Field(ref.Val)
	if f == nil {
		return nil
	}

	// Check if we need to perform a cast. Performing a cast in the
	// engine (if it is possible) is much more efficient than an automatic cast.
	if ref.Type != cnosql.Unknown && ref.Type != cnosql.AnyField && ref.Type != f.Type {
		switch ref.Type {
		case cnosql.Float:
			switch f.Type {
			case cnosql.Integer:
				cur := e.buildIntegerCursor(ctx, metric, seriesKey, ref.Val, opt)
				return &floatCastIntegerCursor{cursor: cur}
			case cnosql.Unsigned:
				cur := e.buildUnsignedCursor(ctx, metric, seriesKey, ref.Val, opt)
				return &floatCastUnsignedCursor{cursor: cur}
			}
		case cnosql.Integer:
			switch f.Type {
			case cnosql.Float:
				cur := e.buildFloatCursor(ctx, metric, seriesKey, ref.Val, opt)
				return &integerCastFloatCursor{cursor: cur}
			case cnosql.Unsigned:
				cur := e.buildUnsignedCursor(ctx, metric, seriesKey, ref.Val, opt)
				return &integerCastUnsignedCursor{cursor: cur}
			}
		case cnosql.Unsigned:
			switch f.Type {
			case cnosql.Float:
				cur := e.buildFloatCursor(ctx, metric, seriesKey, ref.Val, opt)
				return &unsignedCastFloatCursor{cursor: cur}
			case cnosql.Integer:
				cur := e.buildIntegerCursor(ctx, metric, seriesKey, ref.Val, opt)
				return &unsignedCastIntegerCursor{cursor: cur}
			}
		}
		return nil
	}

	// Return appropriate cursor based on type.
	switch f.Type {
	case cnosql.Float:
		return e.buildFloatCursor(ctx, metric, seriesKey, ref.Val, opt)
	case cnosql.Integer:
		return e.buildIntegerCursor(ctx, metric, seriesKey, ref.Val, opt)
	case cnosql.Unsigned:
		return e.buildUnsignedCursor(ctx, metric, seriesKey, ref.Val, opt)
	case cnosql.String:
		return e.buildStringCursor(ctx, metric, seriesKey, ref.Val, opt)
	case cnosql.Boolean:
		return e.buildBooleanCursor(ctx, metric, seriesKey, ref.Val, opt)
	default:
		panic("unreachable")
	}
}

func matchTagValues(tags model.Tags, condition cnosql.Expr) []string {
	if condition == nil {
		return tags.Values()
	}

	// Populate map with tag values.
	data := map[string]interface{}{}
	for _, tag := range tags {
		data[string(tag.Key)] = string(tag.Value)
	}

	// Match against each specific tag.
	var values []string
	for _, tag := range tags {
		data["_tagKey"] = string(tag.Key)
		if cnosql.EvalBool(condition, data) {
			values = append(values, string(tag.Value))
		}
	}
	return values
}

// IteratorCost produces the cost of an iterator.
func (e *Engine) IteratorCost(metric string, opt query.IteratorOptions) (query.IteratorCost, error) {
	// Determine if this metric exists. If it does not, then no shards are
	// accessed to begin with.
	if exists, err := e.index.MetricExists([]byte(metric)); err != nil {
		return query.IteratorCost{}, err
	} else if !exists {
		return query.IteratorCost{}, nil
	}

	// Determine all of the tag sets for this query.
	indexSet := tsdb.IndexSet{Indexes: []tsdb.Index{e.index}, SeriesFile: e.sfile}
	tagSets, err := indexSet.TagSets(e.sfile, []byte(metric), opt)
	if err != nil {
		return query.IteratorCost{}, err
	}

	// Attempt to retrieve the ref from the main expression (if it exists).
	var ref *cnosql.VarRef
	if opt.Expr != nil {
		if v, ok := opt.Expr.(*cnosql.VarRef); ok {
			ref = v
		} else if call, ok := opt.Expr.(*cnosql.Call); ok {
			if len(call.Args) > 0 {
				ref, _ = call.Args[0].(*cnosql.VarRef)
			}
		}
	}

	// Count the number of series concatenated from the tag set.
	cost := query.IteratorCost{NumShards: 1}
	for _, t := range tagSets {
		cost.NumSeries += int64(len(t.SeriesKeys))
		for i, key := range t.SeriesKeys {
			// Retrieve the cost for the main expression (if it exists).
			if ref != nil {
				c := e.seriesCost(key, ref.Val, opt.StartTime, opt.EndTime)
				cost = cost.Combine(c)
			}

			// Retrieve the cost for every auxiliary field since these are also
			// iterators that we may have to look through.
			// We may want to separate these though as we are unlikely to incur
			// anywhere close to the full costs of the auxiliary iterators because
			// many of the selected values are usually skipped.
			for _, ref := range opt.Aux {
				c := e.seriesCost(key, ref.Val, opt.StartTime, opt.EndTime)
				cost = cost.Combine(c)
			}

			// Retrieve the expression names in the condition (if there is a condition).
			// We will also create cursors for these too.
			if t.Filters[i] != nil {
				refs := cnosql.ExprNames(t.Filters[i])
				for _, ref := range refs {
					c := e.seriesCost(key, ref.Val, opt.StartTime, opt.EndTime)
					cost = cost.Combine(c)
				}
			}
		}
	}
	return cost, nil
}

// Type returns FieldType for a series.  If the series does not
// exist, ErrUnkownFieldType is returned.
func (e *Engine) Type(series []byte) (model.FieldType, error) {
	if typ, err := e.Cache.Type(series); err == nil {
		return typ, nil
	}

	typ, err := e.FileStore.Type(series)
	if err != nil {
		return 0, err
	}
	switch typ {
	case BlockFloat64:
		return model.Float, nil
	case BlockInteger:
		return model.Integer, nil
	case BlockUnsigned:
		return model.Unsigned, nil
	case BlockString:
		return model.String, nil
	case BlockBoolean:
		return model.Boolean, nil
	}
	return 0, tsdb.ErrUnknownFieldType
}

func (e *Engine) seriesCost(seriesKey, field string, tmin, tmax int64) query.IteratorCost {
	key := SeriesFieldKeyBytes(seriesKey, field)
	c := e.FileStore.Cost(key, tmin, tmax)

	// Retrieve the range of values within the cache.
	cacheValues := e.Cache.Values(key)
	c.CachedValues = int64(len(cacheValues.Include(tmin, tmax)))
	return c
}

// SeriesFieldKey combine a series key and field name for a unique string to be hashed to a numeric ID.
func SeriesFieldKey(seriesKey, field string) string {
	return seriesKey + keyFieldSeparator + field
}

func SeriesFieldKeyBytes(seriesKey, field string) []byte {
	b := make([]byte, len(seriesKey)+len(keyFieldSeparator)+len(field))
	i := copy(b[:], seriesKey)
	i += copy(b[i:], keyFieldSeparatorBytes)
	copy(b[i:], field)
	return b
}

var (
	blockToFieldType = [8]cnosql.DataType{
		BlockFloat64:  cnosql.Float,
		BlockInteger:  cnosql.Integer,
		BlockBoolean:  cnosql.Boolean,
		BlockString:   cnosql.String,
		BlockUnsigned: cnosql.Unsigned,
		5:             cnosql.Unknown,
		6:             cnosql.Unknown,
		7:             cnosql.Unknown,
	}
)

func BlockTypeToCnosQLDataType(typ byte) cnosql.DataType { return blockToFieldType[typ&7] }

// SeriesAndFieldFromCompositeKey returns the series key and the field key extracted from the composite key.
func SeriesAndFieldFromCompositeKey(key []byte) ([]byte, []byte) {
	sep := bytes.Index(key, keyFieldSeparatorBytes)
	if sep == -1 {
		// No field???
		return key, nil
	}
	return key[:sep], key[sep+len(keyFieldSeparator):]
}

func varRefSliceContains(a []cnosql.VarRef, v string) bool {
	for _, ref := range a {
		if ref.Val == v {
			return true
		}
	}
	return false
}

func varRefSliceRemove(a []cnosql.VarRef, v string) []cnosql.VarRef {
	if !varRefSliceContains(a, v) {
		return a
	}

	other := make([]cnosql.VarRef, 0, len(a))
	for _, ref := range a {
		if ref.Val != v {
			other = append(other, ref)
		}
	}
	return other
}
