package coordinator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	cnosdb "db"
	"db/meta"
	"db/model"
	"db/parser/cnosql"
	"db/pkg/tracing"
	"db/pkg/tracing/fields"
	"db/query"
	"db/tsdb"
)

// ErrDatabaseNameRequired is returned when executing statements that require a database,
// when a database has not been provided.
var ErrDatabaseNameRequired = errors.New("database name required")

type pointsWriter interface {
	WritePointsInto(*IntoWriteRequest) error
}

// StatementExecutor executes a statement in the query.
type StatementExecutor struct {
	MetaClient *meta.Client

	// TaskManager holds the StatementExecutor that handles task-related commands.
	TaskManager query.StatementExecutor

	// TSDB storage for local node.
	TSDBStore *tsdb.Store

	// ShardMapper for mapping shards when executing a SELECT statement.
	ShardMapper query.ShardMapper

	// Used for rewriting points back into system for SELECT INTO statements.
	PointsWriter interface {
		WritePointsInto(*IntoWriteRequest) error
	}

	// Select statement limits
	MaxSelectPointN   int
	MaxSelectSeriesN  int
	MaxSelectBucketsN int
}

// ExecuteStatement executes the given statement with the given execution context.
func (e *StatementExecutor) ExecuteStatement(ctx *query.ExecutionContext, stmt cnosql.Statement) error {
	// Select statements are handled separately so that they can be streamed.
	if stmt, ok := stmt.(*cnosql.SelectStatement); ok {
		return e.executeSelectStatement(stmt, ctx)
	}

	var rows model.Rows
	var messages []*query.Message
	var err error
	switch stmt := stmt.(type) {
	case *cnosql.AlterTimeToLiveStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeAlterTimeToLiveStatement(stmt)
	case *cnosql.CreateContinuousQueryStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeCreateContinuousQueryStatement(stmt)
	case *cnosql.CreateDatabaseStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeCreateDatabaseStatement(stmt)
	case *cnosql.CreateTimeToLiveStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeCreateTimeToLiveStatement(stmt)
	case *cnosql.DeleteSeriesStatement:
		err = e.executeDeleteSeriesStatement(stmt, ctx.Database)
	case *cnosql.DropContinuousQueryStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeDropContinuousQueryStatement(stmt)
	case *cnosql.DropDatabaseStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeDropDatabaseStatement(stmt)
	case *cnosql.DropMetricStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeDropMetricStatement(stmt, ctx.Database)
	case *cnosql.DropSeriesStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeDropSeriesStatement(stmt, ctx.Database)
	case *cnosql.DropTimeToLiveStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeDropTimeToLiveStatement(stmt)
	case *cnosql.DropShardStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeDropShardStatement(stmt)
	case *cnosql.ExplainStatement:
		if stmt.Analyze {
			rows, err = e.executeExplainAnalyzeStatement(ctx, stmt)
		} else {
			rows, err = e.executeExplainStatement(ctx, stmt)
		}
	case *cnosql.ShowDatabasesStatement:
		rows, err = e.executeShowDatabasesStatement(ctx, stmt)
	case *cnosql.ShowMetricsStatement:
		return e.executeShowMetricsStatement(ctx, stmt)
	case *cnosql.ShowTimeToLivesStatement:
		rows, err = e.executeShowTimeToLivesStatement(stmt)
	case *cnosql.ShowShardsStatement:
		rows, err = e.executeShowShardsStatement(stmt)
	case *cnosql.ShowTagKeysStatement:
		return e.executeShowTagKeys(ctx, stmt)
	case *cnosql.ShowTagValuesStatement:
		return e.executeShowTagValues(ctx, stmt)
	case *cnosql.ShowQueriesStatement, *cnosql.KillQueryStatement:
		// Send query related statements to the task manager.
		return e.TaskManager.ExecuteStatement(ctx, stmt)
	default:
		return query.ErrInvalidQuery
	}

	if err != nil {
		return err
	}

	return ctx.Send(&query.Result{
		Series:   rows,
		Messages: messages,
	})
}

func (e *StatementExecutor) executeAlterTimeToLiveStatement(stmt *cnosql.AlterTimeToLiveStatement) error {
	rpu := &meta.TimeToLiveUpdate{
		Duration:       stmt.Duration,
		ReplicaN:       stmt.Replication,
		RegionDuration: stmt.RegionDuration,
	}

	// Update the time-to-live.
	return e.MetaClient.UpdateTimeToLive(stmt.Database, stmt.Name, rpu, stmt.Default)
}

func (e *StatementExecutor) executeCreateContinuousQueryStatement(q *cnosql.CreateContinuousQueryStatement) error {
	// Verify that time-to-lives exist.
	var err error
	verifyRPFn := func(n cnosql.Node) {
		if err != nil {
			return
		}
		switch m := n.(type) {
		case *cnosql.Metric:
			var ttl *meta.TimeToLiveInfo
			if ttl, err = e.MetaClient.TimeToLive(m.Database, m.TimeToLive); err != nil {
				return
			} else if ttl == nil {
				err = fmt.Errorf("%s: %s.%s", meta.ErrTimeToLiveNotFound, m.Database, m.TimeToLive)
			}
		default:
			return
		}
	}

	cnosql.WalkFunc(q, verifyRPFn)

	if err != nil {
		return err
	}

	return e.MetaClient.CreateContinuousQuery(q.Database, q.Name, q.String())
}

func (e *StatementExecutor) executeCreateDatabaseStatement(stmt *cnosql.CreateDatabaseStatement) error {
	if !meta.ValidName(stmt.Name) {
		// TODO This should probably be in `(*meta.Data).CreateDatabase`
		// but can't go there until 1.1 is used everywhere
		return meta.ErrInvalidName
	}

	if !stmt.TimeToLiveCreate {
		_, err := e.MetaClient.CreateDatabase(stmt.Name)
		return err
	}

	// If we're doing, for example, CREATE DATABASE "db" WITH DURATION 1d then
	// the name will not yet be set. We only need to validate non-empty
	// time-to-live names, such as in the statement:
	// 	CREATE DATABASE "db" WITH DURATION 1d NAME "xyz"
	if stmt.TimeToLiveName != "" && !meta.ValidName(stmt.TimeToLiveName) {
		return meta.ErrInvalidName
	}

	spec := meta.TimeToLiveSpec{
		Name:           stmt.TimeToLiveName,
		Duration:       stmt.TimeToLiveDuration,
		ReplicaN:       stmt.TimeToLiveReplication,
		RegionDuration: stmt.TimeToLiveRegionDuration,
	}
	_, err := e.MetaClient.CreateDatabaseWithTimeToLive(stmt.Name, &spec)
	return err
}

func (e *StatementExecutor) executeCreateTimeToLiveStatement(stmt *cnosql.CreateTimeToLiveStatement) error {
	if !meta.ValidName(stmt.Name) {
		// TODO This should probably be in `(*meta.Data).CreateTimeToLive`
		// but can't go there until 1.1 is used everywhere
		return meta.ErrInvalidName
	}

	spec := meta.TimeToLiveSpec{
		Name:           stmt.Name,
		Duration:       &stmt.Duration,
		ReplicaN:       &stmt.Replication,
		RegionDuration: stmt.RegionDuration,
	}

	// Create new time-to-live.
	_, err := e.MetaClient.CreateTimeToLive(stmt.Database, &spec, stmt.Default)
	return err
}

func (e *StatementExecutor) executeDeleteSeriesStatement(stmt *cnosql.DeleteSeriesStatement, database string) error {
	if dbi := e.MetaClient.Database(database); dbi == nil {
		return query.ErrDatabaseNotFound(database)
	}

	// Convert "now()" to current time.
	stmt.Condition = cnosql.Reduce(stmt.Condition, &cnosql.NowValuer{Now: time.Now().UTC()})

	// Locally delete the series.
	return e.TSDBStore.DeleteSeries(database, stmt.Sources, stmt.Condition)
}

func (e *StatementExecutor) executeDropContinuousQueryStatement(q *cnosql.DropContinuousQueryStatement) error {
	return e.MetaClient.DropContinuousQuery(q.Database, q.Name)
}

// executeDropDatabaseStatement drops a database from the cluster.
// It does not return an error if the database was not found on any of
// the nodes, or in the Meta store.
func (e *StatementExecutor) executeDropDatabaseStatement(stmt *cnosql.DropDatabaseStatement) error {
	if e.MetaClient.Database(stmt.Name) == nil {
		return nil
	}

	// Locally delete the datababse.
	if err := e.TSDBStore.DeleteDatabase(stmt.Name); err != nil {
		return err
	}

	// Remove the database from the Meta Store.
	return e.MetaClient.DropDatabase(stmt.Name)
}

func (e *StatementExecutor) executeDropMetricStatement(stmt *cnosql.DropMetricStatement, database string) error {
	if dbi := e.MetaClient.Database(database); dbi == nil {
		return query.ErrDatabaseNotFound(database)
	}

	// Locally drop the metric
	return e.TSDBStore.DeleteMetric(database, stmt.Name)
}

func (e *StatementExecutor) executeDropSeriesStatement(stmt *cnosql.DropSeriesStatement, database string) error {
	if dbi := e.MetaClient.Database(database); dbi == nil {
		return query.ErrDatabaseNotFound(database)
	}

	// Check for time in WHERE clause (not supported).
	if cnosql.HasTimeExpr(stmt.Condition) {
		return errors.New("DROP SERIES doesn't support time in WHERE clause")
	}

	// Locally drop the series.
	return e.TSDBStore.DeleteSeries(database, stmt.Sources, stmt.Condition)
}

func (e *StatementExecutor) executeDropShardStatement(stmt *cnosql.DropShardStatement) error {
	// Locally delete the shard.
	if err := e.TSDBStore.DeleteShard(stmt.ID); err != nil {
		return err
	}

	// Remove the shard reference from the Meta Store.
	return e.MetaClient.DropShard(stmt.ID)
}

func (e *StatementExecutor) executeDropTimeToLiveStatement(stmt *cnosql.DropTimeToLiveStatement) error {
	dbi := e.MetaClient.Database(stmt.Database)
	if dbi == nil {
		return nil
	}

	if dbi.TimeToLive(stmt.Name) == nil {
		return nil
	}

	// Locally drop the time-to-live.
	if err := e.TSDBStore.DeleteTimeToLive(stmt.Database, stmt.Name); err != nil {
		return err
	}

	return e.MetaClient.DropTimeToLive(stmt.Database, stmt.Name)
}

func (e *StatementExecutor) executeExplainStatement(ctx *query.ExecutionContext, q *cnosql.ExplainStatement) (model.Rows, error) {
	opt := query.SelectOptions{
		MaxSeriesN:  e.MaxSelectSeriesN,
		MaxBucketsN: e.MaxSelectBucketsN,
	}

	// Prepare the query for execution, but do not actually execute it.
	// This should perform any needed substitutions.
	p, err := query.Prepare(q.Statement, e.ShardMapper, opt)
	if err != nil {
		return nil, err
	}
	defer p.Close()

	plan, err := p.Explain()
	if err != nil {
		return nil, err
	}
	plan = strings.TrimSpace(plan)

	row := &model.Row{
		Columns: []string{"QUERY PLAN"},
	}
	for _, s := range strings.Split(plan, "\n") {
		row.Values = append(row.Values, []interface{}{s})
	}
	return model.Rows{row}, nil
}

func (e *StatementExecutor) executeExplainAnalyzeStatement(ectx *query.ExecutionContext, q *cnosql.ExplainStatement) (model.Rows, error) {
	stmt := q.Statement
	t, span := tracing.NewTrace("select")
	ctx := tracing.NewContextWithTrace(ectx, t)
	ctx = tracing.NewContextWithSpan(ctx, span)
	var aux query.Iterators
	ctx = query.NewContextWithIterators(ctx, &aux)
	start := time.Now()

	cur, err := e.createIterators(ctx, stmt, ectx.ExecutionOptions)
	if err != nil {
		return nil, err
	}

	iterTime := time.Since(start)

	// Generate a row emitter from the iterator set.
	em := query.NewEmitter(cur, ectx.ChunkSize)

	// Emit rows to the results channel.
	var writeN int64
	for {
		var row *model.Row
		row, _, err = em.Emit()
		if err != nil {
			goto CLEANUP
		} else if row == nil {
			// Check if the query was interrupted while emitting.
			select {
			case <-ectx.Done():
				err = ectx.Err()
				goto CLEANUP
			default:
			}
			break
		}

		writeN += int64(len(row.Values))
	}

CLEANUP:
	em.Close()
	if err != nil {
		return nil, err
	}

	// close auxiliary iterators deterministically to finalize any captured metrics
	aux.Close()

	totalTime := time.Since(start)
	span.MergeFields(
		fields.Duration("total_time", totalTime),
		fields.Duration("planning_time", iterTime),
		fields.Duration("execution_time", totalTime-iterTime),
	)
	span.Finish()

	row := &model.Row{
		Columns: []string{"EXPLAIN ANALYZE"},
	}
	for _, s := range strings.Split(t.Tree().String(), "\n") {
		row.Values = append(row.Values, []interface{}{s})
	}

	return model.Rows{row}, nil
}

func (e *StatementExecutor) executeSelectStatement(stmt *cnosql.SelectStatement, ctx *query.ExecutionContext) error {
	cur, err := e.createIterators(ctx, stmt, ctx.ExecutionOptions)
	if err != nil {
		return err
	}

	// Generate a row emitter from the iterator set.
	em := query.NewEmitter(cur, ctx.ChunkSize)
	defer em.Close()

	// Emit rows to the results channel.
	var writeN int64
	var emitted bool

	var pointsWriter *BufferedPointsWriter
	if stmt.Target != nil {
		pointsWriter = NewBufferedPointsWriter(e.PointsWriter, stmt.Target.Metric.Database, stmt.Target.Metric.TimeToLive, 10000)
	}

	for {
		row, partial, err := em.Emit()
		if err != nil {
			return err
		} else if row == nil {
			// Check if the query was interrupted while emitting.
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			break
		}

		// Write points back into system for INTO statements.
		if stmt.Target != nil {
			n, err := e.writeInto(pointsWriter, stmt, row)
			if err != nil {
				return err
			}
			writeN += n
			continue
		}

		result := &query.Result{
			Series:  []*model.Row{row},
			Partial: partial,
		}

		// Send results or exit if closing.
		if err := ctx.Send(result); err != nil {
			return err
		}

		emitted = true
	}

	// Flush remaining points and emit write count if an INTO statement.
	if stmt.Target != nil {
		if err := pointsWriter.Flush(); err != nil {
			return err
		}

		var messages []*query.Message
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}

		return ctx.Send(&query.Result{
			Messages: messages,
			Series: []*model.Row{{
				Name:    "result",
				Columns: []string{"time", "written"},
				Values:  [][]interface{}{{time.Unix(0, 0).UTC(), writeN}},
			}},
		})
	}

	// Always emit at least one result.
	if !emitted {
		return ctx.Send(&query.Result{
			Series: make([]*model.Row, 0),
		})
	}

	return nil
}

func (e *StatementExecutor) createIterators(ctx context.Context, stmt *cnosql.SelectStatement, opt query.ExecutionOptions) (query.Cursor, error) {
	sopt := query.SelectOptions{
		MaxSeriesN:  e.MaxSelectSeriesN,
		MaxPointN:   e.MaxSelectPointN,
		MaxBucketsN: e.MaxSelectBucketsN,
	}

	// Create a set of iterators from a selection.
	cur, err := query.Select(ctx, stmt, e.ShardMapper, sopt)
	if err != nil {
		return nil, err
	}
	return cur, nil
}

func (e *StatementExecutor) executeShowDatabasesStatement(ctx *query.ExecutionContext, q *cnosql.ShowDatabasesStatement) (model.Rows, error) {
	dis := e.MetaClient.Databases()
	row := &model.Row{Name: "databases", Columns: []string{"name"}}
	for _, di := range dis {
		// Only include databases that the user is authorized to read or write.
		row.Values = append(row.Values, []interface{}{di.Name})
	}

	return []*model.Row{row}, nil
}

func (e *StatementExecutor) executeShowMetricsStatement(ctx *query.ExecutionContext, q *cnosql.ShowMetricsStatement) error {
	if q.Database == "" {
		return ErrDatabaseNameRequired
	}

	names, err := e.TSDBStore.MetricNames(q.Database, q.Condition)
	if err != nil || len(names) == 0 {
		return ctx.Send(&query.Result{
			Err: err,
		})
	}

	if q.Offset > 0 {
		if q.Offset >= len(names) {
			names = nil
		} else {
			names = names[q.Offset:]
		}
	}

	if q.Limit > 0 {
		if q.Limit < len(names) {
			names = names[:q.Limit]
		}
	}

	values := make([][]interface{}, len(names))
	for i, name := range names {
		values[i] = []interface{}{string(name)}
	}

	if len(values) == 0 {
		return ctx.Send(&query.Result{})
	}

	return ctx.Send(&query.Result{
		Series: []*model.Row{{
			Name:    "metrics",
			Columns: []string{"name"},
			Values:  values,
		}},
	})
}

func (e *StatementExecutor) executeShowTimeToLivesStatement(q *cnosql.ShowTimeToLivesStatement) (model.Rows, error) {
	if q.Database == "" {
		return nil, ErrDatabaseNameRequired
	}

	di := e.MetaClient.Database(q.Database)
	if di == nil {
		return nil, cnosdb.ErrDatabaseNotFound(q.Database)
	}

	row := &model.Row{Columns: []string{"name", "duration", "shardGroupDuration", "replicaN", "default"}}
	for _, rpi := range di.TimeToLives {
		row.Values = append(row.Values, []interface{}{rpi.Name, rpi.Duration.String(), rpi.RegionDuration.String(), rpi.ReplicaN, di.DefaultTimeToLive == rpi.Name})
	}
	return []*model.Row{row}, nil
}

func (e *StatementExecutor) executeShowShardsStatement(stmt *cnosql.ShowShardsStatement) (model.Rows, error) {
	dis := e.MetaClient.Databases()

	rows := []*model.Row{}
	for _, di := range dis {
		row := &model.Row{Columns: []string{"id", "database", "retention_policy", "shard_group", "start_time", "end_time", "expiry_time", "owners"}, Name: di.Name}
		for _, rpi := range di.TimeToLives {
			for _, sgi := range rpi.Regions {
				// Shards associated with deleted shard groups are effectively deleted.
				// Don't list them.
				if sgi.Deleted() {
					continue
				}

				for _, si := range sgi.Shards {
					ownerIDs := make([]uint64, len(si.Owners))
					for i, owner := range si.Owners {
						ownerIDs[i] = owner.NodeID
					}

					row.Values = append(row.Values, []interface{}{
						si.ID,
						di.Name,
						rpi.Name,
						sgi.ID,
						sgi.StartTime.UTC().Format(time.RFC3339),
						sgi.EndTime.UTC().Format(time.RFC3339),
						sgi.EndTime.Add(rpi.Duration).UTC().Format(time.RFC3339),
						joinUint64(ownerIDs),
					})
				}
			}
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (e *StatementExecutor) executeShowRegionsStatement(stmt *cnosql.ShowRegionsStatement) (model.Rows, error) {
	dis := e.MetaClient.Databases()

	row := &model.Row{Columns: []string{"id", "database", "retention_policy", "start_time", "end_time", "expiry_time"}, Name: "shard groups"}
	for _, di := range dis {
		for _, rpi := range di.TimeToLives {
			for _, sgi := range rpi.Regions {
				// Shards associated with deleted shard groups are effectively deleted.
				// Don't list them.
				if sgi.Deleted() {
					continue
				}

				row.Values = append(row.Values, []interface{}{
					sgi.ID,
					di.Name,
					rpi.Name,
					sgi.StartTime.UTC().Format(time.RFC3339),
					sgi.EndTime.UTC().Format(time.RFC3339),
					sgi.EndTime.Add(rpi.Duration).UTC().Format(time.RFC3339),
				})
			}
		}
	}

	return []*model.Row{row}, nil
}

func (e *StatementExecutor) executeShowTagKeys(ctx *query.ExecutionContext, q *cnosql.ShowTagKeysStatement) error {
	if q.Database == "" {
		return ErrDatabaseNameRequired
	}

	// Determine shard set based on database and time range.
	// SHOW TAG KEYS returns all tag keys for the default time-to-live.
	di := e.MetaClient.Database(q.Database)
	if di == nil {
		return fmt.Errorf("database not found: %s", q.Database)
	}

	// Determine appropriate time range. If one or fewer time boundaries provided
	// then min/max possible time should be used instead.
	valuer := &cnosql.NowValuer{Now: time.Now()}
	cond, timeRange, err := cnosql.ConditionExpr(q.Condition, valuer)
	if err != nil {
		return err
	}

	// Get all shards for all time-to-lives.
	var allGroups []meta.RegionInfo
	for _, rpi := range di.TimeToLives {
		sgis, err := e.MetaClient.RegionsByTimeRange(q.Database, rpi.Name, timeRange.MinTime(), timeRange.MaxTime())
		if err != nil {
			return err
		}
		allGroups = append(allGroups, sgis...)
	}

	var shardIDs []uint64
	for _, sgi := range allGroups {
		for _, si := range sgi.Shards {
			shardIDs = append(shardIDs, si.ID)
		}
	}

	tagKeys, err := e.TSDBStore.TagKeys(shardIDs, cond)
	if err != nil {
		return ctx.Send(&query.Result{
			Err: err,
		})
	}

	emitted := false
	for _, m := range tagKeys {
		keys := m.Keys

		if q.Offset > 0 {
			if q.Offset >= len(keys) {
				keys = nil
			} else {
				keys = keys[q.Offset:]
			}
		}
		if q.Limit > 0 && q.Limit < len(keys) {
			keys = keys[:q.Limit]
		}

		if len(keys) == 0 {
			continue
		}

		row := &model.Row{
			Name:    m.Metric,
			Columns: []string{"tagKey"},
			Values:  make([][]interface{}, len(keys)),
		}
		for i, key := range keys {
			row.Values[i] = []interface{}{key}
		}

		if err := ctx.Send(&query.Result{
			Series: []*model.Row{row},
		}); err != nil {
			return err
		}
		emitted = true
	}

	// Ensure at least one result is emitted.
	if !emitted {
		return ctx.Send(&query.Result{})
	}
	return nil
}

func (e *StatementExecutor) executeShowTagValues(ctx *query.ExecutionContext, q *cnosql.ShowTagValuesStatement) error {
	if q.Database == "" {
		return ErrDatabaseNameRequired
	}

	// Determine shard set based on database and time range.
	// SHOW TAG VALUES returns all tag values for the default time-to-live.
	di := e.MetaClient.Database(q.Database)
	if di == nil {
		return fmt.Errorf("database not found: %s", q.Database)
	}

	// Determine appropriate time range. If one or fewer time boundaries provided
	// then min/max possible time should be used instead.
	valuer := &cnosql.NowValuer{Now: time.Now()}
	cond, timeRange, err := cnosql.ConditionExpr(q.Condition, valuer)
	if err != nil {
		return err
	}

	// Get all shards for all time-to-lives.
	var allGroups []meta.RegionInfo
	for _, rpi := range di.TimeToLives {
		sgis, err := e.MetaClient.RegionsByTimeRange(q.Database, rpi.Name, timeRange.MinTime(), timeRange.MaxTime())
		if err != nil {
			return err
		}
		allGroups = append(allGroups, sgis...)
	}

	var shardIDs []uint64
	for _, sgi := range allGroups {
		for _, si := range sgi.Shards {
			shardIDs = append(shardIDs, si.ID)
		}
	}

	tagValues, err := e.TSDBStore.TagValues(shardIDs, cond)
	if err != nil {
		return ctx.Send(&query.Result{Err: err})
	}

	emitted := false
	for _, m := range tagValues {
		values := m.Values

		if q.Offset > 0 {
			if q.Offset >= len(values) {
				values = nil
			} else {
				values = values[q.Offset:]
			}
		}

		if q.Limit > 0 {
			if q.Limit < len(values) {
				values = values[:q.Limit]
			}
		}

		if len(values) == 0 {
			continue
		}

		row := &model.Row{
			Name:    m.Metric,
			Columns: []string{"key", "value"},
			Values:  make([][]interface{}, len(values)),
		}
		for i, v := range values {
			row.Values[i] = []interface{}{v.Key, v.Value}
		}

		if err := ctx.Send(&query.Result{
			Series: []*model.Row{row},
		}); err != nil {
			return err
		}
		emitted = true
	}

	// Ensure at least one result is emitted.
	if !emitted {
		return ctx.Send(&query.Result{})
	}
	return nil
}

// BufferedPointsWriter adds buffering to a pointsWriter so that SELECT INTO queries
// write their points to the destination in batches.
type BufferedPointsWriter struct {
	w          pointsWriter
	buf        []model.Point
	database   string
	timeToLive string
}

// NewBufferedPointsWriter returns a new BufferedPointsWriter.
func NewBufferedPointsWriter(w pointsWriter, database, timeToLive string, capacity int) *BufferedPointsWriter {
	return &BufferedPointsWriter{
		w:          w,
		buf:        make([]model.Point, 0, capacity),
		database:   database,
		timeToLive: timeToLive,
	}
}

// WritePointsInto implements pointsWriter for BufferedPointsWriter.
func (w *BufferedPointsWriter) WritePointsInto(req *IntoWriteRequest) error {
	// Make sure we're buffering points only for the expected destination.
	if req.Database != w.database || req.TimeToLive != w.timeToLive {
		return fmt.Errorf("writer for %s.%s can't write into %s.%s", w.database, w.timeToLive, req.Database, req.TimeToLive)
	}

	for i := 0; i < len(req.Points); {
		// Get the available space in the buffer.
		avail := cap(w.buf) - len(w.buf)

		// Calculate number of points to copy into the buffer.
		n := len(req.Points[i:])
		if n > avail {
			n = avail
		}

		// Copy points into buffer.
		w.buf = append(w.buf, req.Points[i:n+i]...)

		// Advance the index by number of points copied.
		i += n

		// If buffer is full, flush points to underlying writer.
		if len(w.buf) == cap(w.buf) {
			if err := w.Flush(); err != nil {
				return err
			}
		}
	}

	return nil
}

// Flush writes all buffered points to the underlying writer.
func (w *BufferedPointsWriter) Flush() error {
	if len(w.buf) == 0 {
		return nil
	}

	if err := w.w.WritePointsInto(&IntoWriteRequest{
		Database:   w.database,
		TimeToLive: w.timeToLive,
		Points:     w.buf,
	}); err != nil {
		return err
	}

	// Clear the buffer.
	w.buf = w.buf[:0]

	return nil
}

// Len returns the number of points buffered.
func (w *BufferedPointsWriter) Len() int { return len(w.buf) }

// Cap returns the capacity (in points) of the buffer.
func (w *BufferedPointsWriter) Cap() int { return cap(w.buf) }

func (e *StatementExecutor) writeInto(w pointsWriter, stmt *cnosql.SelectStatement, row *model.Row) (n int64, err error) {
	if stmt.Target.Metric.Database == "" {
		return 0, errNoDatabaseInTarget
	}

	// It might seem a bit weird that this is where we do this, since we will have to
	// convert rows back to points. The Executors (both aggregate and raw) are complex
	// enough that changing them to write back to the DB is going to be clumsy
	//
	// it might seem weird to have the write be in the Executor, but the interweaving of
	// limitedRowWriter and ExecuteAggregate/Raw makes it ridiculously hard to make sure that the
	// results will be the same as when queried normally.
	name := stmt.Target.Metric.Name
	if name == "" {
		name = row.Name
	}

	points, err := convertRowToPoints(name, row)
	if err != nil {
		return 0, err
	}

	if err := w.WritePointsInto(&IntoWriteRequest{
		Database:   stmt.Target.Metric.Database,
		TimeToLive: stmt.Target.Metric.TimeToLive,
		Points:     points,
	}); err != nil {
		return 0, err
	}

	return int64(len(points)), nil
}

var errNoDatabaseInTarget = errors.New("no database in target")

// convertRowToPoints will convert a query result Row into Points that can be written back in.
func convertRowToPoints(metricName string, row *model.Row) ([]model.Point, error) {
	// figure out which parts of the result are the time and which are the fields
	timeIndex := -1
	fieldIndexes := make(map[string]int)
	for i, c := range row.Columns {
		if c == "time" {
			timeIndex = i
		} else {
			fieldIndexes[c] = i
		}
	}

	if timeIndex == -1 {
		return nil, errors.New("error finding time index in result")
	}

	points := make([]model.Point, 0, len(row.Values))
	for _, v := range row.Values {
		vals := make(map[string]interface{})
		for fieldName, fieldIndex := range fieldIndexes {
			val := v[fieldIndex]
			// Check specifically for nil or a NullFloat. This is because
			// the NullFloat represents float numbers that don't have an internal representation
			// (like NaN) that cannot be written back, but will not equal nil so there will be
			// an attempt to write them if we do not check for it.
			if val != nil && val != query.NullFloat {
				vals[fieldName] = v[fieldIndex]
			}
		}

		p, err := model.NewPoint(metricName, model.NewTags(row.Tags), vals, v[timeIndex].(time.Time))
		if err != nil {
			// Drop points that can't be stored
			continue
		}

		points = append(points, p)
	}

	return points, nil
}

// NormalizeStatement adds a default database and policy to the metrics in statement.
// Parameter defaultTimeToLive can be "".
func (e *StatementExecutor) NormalizeStatement(stmt cnosql.Statement, defaultDatabase, defaultTimeToLive string) (err error) {
	cnosql.WalkFunc(stmt, func(node cnosql.Node) {
		if err != nil {
			return
		}
		switch node := node.(type) {
		case *cnosql.ShowTimeToLivesStatement:
			if node.Database == "" {
				node.Database = defaultDatabase
			}
		case *cnosql.ShowMetricsStatement:
			if node.Database == "" {
				node.Database = defaultDatabase
			}
		case *cnosql.ShowTagKeysStatement:
			if node.Database == "" {
				node.Database = defaultDatabase
			}
		case *cnosql.ShowTagValuesStatement:
			if node.Database == "" {
				node.Database = defaultDatabase
			}
		case *cnosql.Metric:
			switch stmt.(type) {
			case *cnosql.DropSeriesStatement, *cnosql.DeleteSeriesStatement:
				// DB and TTL not supported by these statements so don't rewrite into invalid
				// statements
			default:
				err = e.normalizeMetric(node, defaultDatabase, defaultTimeToLive)
			}
		}
	})
	return
}

func (e *StatementExecutor) normalizeMetric(m *cnosql.Metric, defaultDatabase, defaultTimeToLive string) error {
	// Targets (metrics in an INTO clause) can have blank names, which means it will be
	// the same as the metric name it came from in the FROM clause.
	if !m.IsTarget && m.Name == "" && m.SystemIterator == "" && m.Regex == nil {
		return errors.New("invalid metric")
	}

	// Metric does not have an explicit database? Insert default.
	if m.Database == "" {
		m.Database = defaultDatabase
	}

	// The database must now be specified by this point.
	if m.Database == "" {
		return ErrDatabaseNameRequired
	}

	// Find database.
	di := e.MetaClient.Database(m.Database)
	if di == nil {
		return cnosdb.ErrDatabaseNotFound(m.Database)
	}

	// If no time-to-live was specified, use the default.
	if m.TimeToLive == "" {
		if defaultTimeToLive != "" {
			m.TimeToLive = defaultTimeToLive
		} else if di.DefaultTimeToLive != "" {
			m.TimeToLive = di.DefaultTimeToLive
		} else {
			return fmt.Errorf("default time-to-live not set for: %s", di.Name)
		}
	}
	return nil
}

// IntoWriteRequest is a partial copy of cluster.WriteRequest
type IntoWriteRequest struct {
	Database   string
	TimeToLive string
	Points     []model.Point
}

// ShardIteratorCreator is an interface for creating an IteratorCreator to access a specific shard.
type ShardIteratorCreator interface {
	ShardIteratorCreator(id uint64) query.IteratorCreator
}

// joinUint64 returns a comma-delimited string of uint64 numbers.
func joinUint64(a []uint64) string {
	var buf bytes.Buffer
	for i, x := range a {
		buf.WriteString(strconv.FormatUint(x, 10))
		if i < len(a)-1 {
			buf.WriteRune(',')
		}
	}
	return buf.String()
}
