package coordinator

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	cnosdb "db"
	"db/meta"
	"db/model"
	"db/tsdb"

	"go.uber.org/zap"
)

const (
	// DefaultWriteTimeout is the default timeout for a complete write to succeed.
	DefaultWriteTimeout = 10 * time.Second

	// DefaultMaxConcurrentQueries is the maximum number of running queries.
	// A value of zero will make the maximum query limit unlimited.
	DefaultMaxConcurrentQueries = 0

	// DefaultMaxSelectPointN is the maximum number of points a SELECT can process.
	// A value of zero will make the maximum point count unlimited.
	DefaultMaxSelectPointN = 0

	// DefaultMaxSelectSeriesN is the maximum number of series a SELECT can run.
	// A value of zero will make the maximum series count unlimited.
	DefaultMaxSelectSeriesN = 0
)

var (
	// ErrTimeout is returned when a write times out.
	ErrTimeout = errors.New("timeout")

	// ErrPartialWrite is returned when a write partially succeeds but does
	// not meet the requested consistency level.
	ErrPartialWrite = errors.New("partial write")

	// ErrWriteFailed is returned when no writes succeeded.
	ErrWriteFailed = errors.New("write failed")
)

// PointsWriter handles writes across multiple local and remote data nodes.
type PointsWriter struct {
	mu           sync.RWMutex
	closing      chan struct{}
	WriteTimeout time.Duration
	Logger       *zap.Logger

	MetaClient interface {
		Database(name string) (di *meta.DatabaseInfo)
		TimeToLive(database, ttl string) (*meta.TimeToLiveInfo, error)
		CreateRegion(database, ttl string, timestamp time.Time) (*meta.RegionInfo, error)
	}

	TSDBStore interface {
		CreateShard(database, timeToLive string, shardID uint64, enabled bool) error
		WriteToShard(shardID uint64, points []model.Point) error
	}

	subPoints []chan<- *WritePointsRequest
}

// WritePointsRequest represents a request to write point data to the cluster.
type WritePointsRequest struct {
	Database   string
	TimeToLive string
	Points     []model.Point
}

// AddPoint adds a point to the WritePointRequest with field key 'value'
func (w *WritePointsRequest) AddPoint(name string, value interface{}, timestamp time.Time, tags map[string]string) {
	pt, err := model.NewPoint(
		name, model.NewTags(tags), map[string]interface{}{"value": value}, timestamp,
	)
	if err != nil {
		return
	}
	w.Points = append(w.Points, pt)
}

// NewPointsWriter returns a new instance of PointsWriter for a node.
func NewPointsWriter() *PointsWriter {
	return &PointsWriter{
		closing:      make(chan struct{}),
		WriteTimeout: DefaultWriteTimeout,
		Logger:       zap.NewNop(),
	}
}

// ShardMapping contains a mapping of shards to points.
type ShardMapping struct {
	n       int
	Points  map[uint64][]model.Point   // The points associated with a shard ID
	Shards  map[uint64]*meta.ShardInfo // The shards that have been mapped, keyed by shard ID
	Dropped []model.Point              // Points that were dropped
}

// NewShardMapping creates an empty ShardMapping.
func NewShardMapping(n int) *ShardMapping {
	return &ShardMapping{
		n:      n,
		Points: map[uint64][]model.Point{},
		Shards: map[uint64]*meta.ShardInfo{},
	}
}

// MapPoint adds the point to the ShardMapping, associated with the given shardInfo.
func (s *ShardMapping) MapPoint(shardInfo *meta.ShardInfo, p model.Point) {
	if cap(s.Points[shardInfo.ID]) < s.n {
		s.Points[shardInfo.ID] = make([]model.Point, 0, s.n)
	}
	s.Points[shardInfo.ID] = append(s.Points[shardInfo.ID], p)
	s.Shards[shardInfo.ID] = shardInfo
}

// Open opens the communication channel with the point writer.
func (w *PointsWriter) Open() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closing = make(chan struct{})
	return nil
}

// Close closes the communication channel with the point writer.
func (w *PointsWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closing != nil {
		close(w.closing)
	}
	if w.subPoints != nil {
		// 'nil' channels always block so this makes the
		// select statement in WritePoints hit its default case
		// dropping any in-flight writes.
		w.subPoints = nil
	}
	return nil
}

func (w *PointsWriter) AddWriteSubscriber(c chan<- *WritePointsRequest) {
	w.subPoints = append(w.subPoints, c)
}

// WithLogger sets the Logger on w.
func (w *PointsWriter) WithLogger(log *zap.Logger) {
	w.Logger = log.With(zap.String("service", "write"))
}

// WriteStatistics keeps statistics related to the PointsWriter.
type WriteStatistics struct {
	WriteReq           int64
	PointWriteReq      int64
	PointWriteReqLocal int64
	WriteOK            int64
	WriteDropped       int64
	WriteTimeout       int64
	WriteErr           int64
	SubWriteOK         int64
	SubWriteDrop       int64
}

// MapShards maps the points contained in wp to a ShardMapping.  If a point
// maps to a shard group or shard that does not currently exist, it will be
// created before returning the mapping.
func (w *PointsWriter) MapShards(wp *WritePointsRequest) (*ShardMapping, error) {
	ttl, err := w.MetaClient.TimeToLive(wp.Database, wp.TimeToLive)
	if err != nil {
		return nil, err
	} else if ttl == nil {
		return nil, cnosdb.ErrTimeToLiveNotFound(wp.TimeToLive)
	}

	// Holds all the shard groups and shards that are required for writes.
	list := make(sgList, 0, 8)
	min := time.Unix(0, model.MinNanoTime)
	if ttl.Duration > 0 {
		min = time.Now().Add(-ttl.Duration)
	}

	for _, p := range wp.Points {
		// Either the point is outside the scope of the TTL, or we already have
		// a suitable shard group for the point.
		if p.Time().Before(min) || list.Covers(p.Time()) {
			continue
		}

		// No shard groups overlap with the point's time, so we will create
		// a new shard group for this point.
		rg, err := w.MetaClient.CreateRegion(wp.Database, wp.TimeToLive, p.Time())
		if err != nil {
			return nil, err
		}

		if rg == nil {
			return nil, errors.New("nil shard group")
		}
		list = list.Append(*rg)
	}

	mapping := NewShardMapping(len(wp.Points))
	for _, p := range wp.Points {
		rg := list.RegionAt(p.Time())
		if rg == nil {
			// We didn't create a shard group because the point was outside the
			// scope of the TTL.
			mapping.Dropped = append(mapping.Dropped, p)
			continue
		}

		sh := rg.ShardFor(p)
		mapping.MapPoint(&sh, p)
	}
	return mapping, nil
}

// sgList is a wrapper around a meta.RegionInfos where we can also check
// if a given time is covered by any of the shard groups in the list.
type sgList meta.RegionInfos

func (l sgList) Covers(t time.Time) bool {
	if len(l) == 0 {
		return false
	}
	return l.RegionAt(t) != nil
}

// RegionAt attempts to find a shard group that could contain a point
// at the given time.
//
// Shard groups are sorted first according to end time, and then according
// to start time. Therefore, if there are multiple shard groups that match
// this point's time they will be preferred in this order:
//
//  - a shard group with the earliest end time;
//  - (assuming identical end times) the shard group with the earliest start time.
func (l sgList) RegionAt(t time.Time) *meta.RegionInfo {
	idx := sort.Search(len(l), func(i int) bool { return l[i].EndTime.After(t) })

	// We couldn't find a shard group the point falls into.
	if idx == len(l) || t.Before(l[idx].StartTime) {
		return nil
	}
	return &l[idx]
}

// Append appends a shard group to the list, and returns a sorted list.
func (l sgList) Append(sgi meta.RegionInfo) sgList {
	next := append(l, sgi)
	sort.Sort(meta.RegionInfos(next))
	return next
}

// WritePointsInto is a copy of WritePoints that uses a tsdb structure instead of
// a cluster structure for information. This is to avoid a circular dependency.
func (w *PointsWriter) WritePointsInto(p *IntoWriteRequest) error {
	return w.WritePointsPrivileged(p.Database, p.TimeToLive, p.Points)
}

// WritePoints writes the data to the underlying storage. consitencyLevel and user are only used for clustered scenarios
func (w *PointsWriter) WritePoints(database, timeToLive string, points []model.Point) error {
	return w.WritePointsPrivileged(database, timeToLive, points)
}

// WritePointsPrivileged writes the data to the underlying storage, consitencyLevel is only used for clustered scenarios
func (w *PointsWriter) WritePointsPrivileged(database, timeToLive string, points []model.Point) error {
	if timeToLive == "" {
		db := w.MetaClient.Database(database)
		if db == nil {
			return cnosdb.ErrDatabaseNotFound(database)
		}
		timeToLive = db.DefaultTimeToLive
	}

	shardMappings, err := w.MapShards(&WritePointsRequest{Database: database, TimeToLive: timeToLive, Points: points})
	if err != nil {
		return err
	}

	// Write each shard in it's own goroutine and return as soon as one fails.
	ch := make(chan error, len(shardMappings.Points))
	for shardID, points := range shardMappings.Points {
		go func(shard *meta.ShardInfo, database, timeToLive string, points []model.Point) {
			err := w.writeToShard(shard, database, timeToLive, points)
			if err == tsdb.ErrShardDeletion {
				err = tsdb.PartialWriteError{Reason: fmt.Sprintf("shard %d is pending deletion", shard.ID), Dropped: len(points)}
			}
			ch <- err
		}(shardMappings.Shards[shardID], database, timeToLive, points)
	}

	// Send points to subscriptions if possible.
	var ok, dropped int64
	pts := &WritePointsRequest{Database: database, TimeToLive: timeToLive, Points: points}
	// We need to lock just in case the channel is about to be nil'ed
	w.mu.RLock()
	for _, ch := range w.subPoints {
		select {
		case ch <- pts:
			ok++
		default:
			dropped++
		}
	}
	w.mu.RUnlock()

	if err == nil && len(shardMappings.Dropped) > 0 {
		err = tsdb.PartialWriteError{Reason: "points beyond time-to-live", Dropped: len(shardMappings.Dropped)}

	}
	timeout := time.NewTimer(w.WriteTimeout)
	defer timeout.Stop()
	for range shardMappings.Points {
		select {
		case <-w.closing:
			return ErrWriteFailed
		case <-timeout.C:
			// return timeout error to caller
			return ErrTimeout
		case err := <-ch:
			if err != nil {
				return err
			}
		}
	}
	return err
}

// writeToShards writes points to a shard.
func (w *PointsWriter) writeToShard(shard *meta.ShardInfo, database, timeToLive string, points []model.Point) error {
	err := w.TSDBStore.WriteToShard(shard.ID, points)
	if err == nil {
		return nil
	}

	// If this is a partial write error, that is also ok.
	if _, ok := err.(tsdb.PartialWriteError); ok {
		return err
	}

	// If we've written to shard that should exist on the current node, but the store has
	// not actually created this shard, tell it to create it and retry the write
	if err == tsdb.ErrShardNotFound {
		err = w.TSDBStore.CreateShard(database, timeToLive, shard.ID, true)
		if err != nil {
			w.Logger.Info("Write failed", zap.Uint64("shard", shard.ID), zap.Error(err))

			return err
		}
	}
	err = w.TSDBStore.WriteToShard(shard.ID, points)
	if err != nil {
		w.Logger.Info("Write failed", zap.Uint64("shard", shard.ID), zap.Error(err))
		return err
	}

	return nil
}
