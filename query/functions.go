package query

import (
	"container/heap"
	"db/parser/cnosql"
	"math"
	"sort"
)

//go:generate tmpl -data=@tmpldata functions.gen.go.tmpl

// FieldMapper is a FieldMapper that wraps another FieldMapper and exposes
// the functions implemented by the query engine.
type FieldMapper struct {
	cnosql.FieldMapper
}

func (m FieldMapper) CallType(name string, args []cnosql.DataType) (cnosql.DataType, error) {
	if mapper, ok := m.FieldMapper.(cnosql.CallTypeMapper); ok {
		typ, err := mapper.CallType(name, args)
		if err != nil {
			return cnosql.Unknown, err
		} else if typ != cnosql.Unknown {
			return typ, nil
		}
	}

	// Use the default FunctionTypeMapper for the query engine.
	typmap := FunctionTypeMapper{}
	return typmap.CallType(name, args)
}

// CallTypeMapper returns the types for call iterator functions.
// Call iterator functions are commonly implemented within the storage engine
// so this mapper is limited to only the return values of those functions.
type CallTypeMapper struct{}

func (CallTypeMapper) MapType(metric *cnosql.Metric, field string) cnosql.DataType {
	return cnosql.Unknown
}

func (CallTypeMapper) CallType(name string, args []cnosql.DataType) (cnosql.DataType, error) {
	// If the function is not implemented by the embedded field mapper, then
	// see if we implement the function and return the type here.
	switch name {
	case "mean":
		return cnosql.Float, nil
	case "count":
		return cnosql.Integer, nil
	case "min", "max", "sum", "first", "last":
		// TODO: Verify the input type.
		return args[0], nil
	}
	return cnosql.Unknown, nil
}

// FunctionTypeMapper handles the type mapping for all functions implemented by the
// query engine.
type FunctionTypeMapper struct {
	CallTypeMapper
}

func (FunctionTypeMapper) MapType(metric *cnosql.Metric, field string) cnosql.DataType {
	return cnosql.Unknown
}

func (m FunctionTypeMapper) CallType(name string, args []cnosql.DataType) (cnosql.DataType, error) {
	if typ, err := m.CallTypeMapper.CallType(name, args); typ != cnosql.Unknown || err != nil {
		return typ, err
	}

	// Handle functions implemented by the query engine.
	switch name {
	case "median", "integral", "stddev",
		"derivative", "non_negative_derivative",
		"moving_average",
		"exponential_moving_average",
		"double_exponential_moving_average",
		"triple_exponential_moving_average",
		"relative_strength_index",
		"triple_exponential_derivative",
		"kaufmans_efficiency_ratio",
		"kaufmans_adaptive_moving_average",
		"chande_momentum_oscillator",
		"holt_winters", "holt_winters_with_fit":
		return cnosql.Float, nil
	case "elapsed":
		return cnosql.Integer, nil
	default:
		// TODO: Do not use default for this.
		return args[0], nil
	}
}

// FloatMeanReducer calculates the mean of the aggregated points.
type FloatMeanReducer struct {
	sum   float64
	count uint32
}

// NewFloatMeanReducer creates a new FloatMeanReducer.
func NewFloatMeanReducer() *FloatMeanReducer {
	return &FloatMeanReducer{}
}

// AggregateFloat aggregates a point into the reducer.
func (r *FloatMeanReducer) AggregateFloat(p *FloatPoint) {
	if p.Aggregated >= 2 {
		r.sum += p.Value * float64(p.Aggregated)
		r.count += p.Aggregated
	} else {
		r.sum += p.Value
		r.count++
	}
}

// Emit emits the mean of the aggregated points as a single point.
func (r *FloatMeanReducer) Emit() []FloatPoint {
	return []FloatPoint{{
		Time:       ZeroTime,
		Value:      r.sum / float64(r.count),
		Aggregated: r.count,
	}}
}

// IntegerMeanReducer calculates the mean of the aggregated points.
type IntegerMeanReducer struct {
	sum   int64
	count uint32
}

// NewIntegerMeanReducer creates a new IntegerMeanReducer.
func NewIntegerMeanReducer() *IntegerMeanReducer {
	return &IntegerMeanReducer{}
}

// AggregateInteger aggregates a point into the reducer.
func (r *IntegerMeanReducer) AggregateInteger(p *IntegerPoint) {
	if p.Aggregated >= 2 {
		r.sum += p.Value * int64(p.Aggregated)
		r.count += p.Aggregated
	} else {
		r.sum += p.Value
		r.count++
	}
}

// Emit emits the mean of the aggregated points as a single point.
func (r *IntegerMeanReducer) Emit() []FloatPoint {
	return []FloatPoint{{
		Time:       ZeroTime,
		Value:      float64(r.sum) / float64(r.count),
		Aggregated: r.count,
	}}
}

// UnsignedMeanReducer calculates the mean of the aggregated points.
type UnsignedMeanReducer struct {
	sum   uint64
	count uint32
}

// NewUnsignedMeanReducer creates a new UnsignedMeanReducer.
func NewUnsignedMeanReducer() *UnsignedMeanReducer {
	return &UnsignedMeanReducer{}
}

// AggregateUnsigned aggregates a point into the reducer.
func (r *UnsignedMeanReducer) AggregateUnsigned(p *UnsignedPoint) {
	if p.Aggregated >= 2 {
		r.sum += p.Value * uint64(p.Aggregated)
		r.count += p.Aggregated
	} else {
		r.sum += p.Value
		r.count++
	}
}

// Emit emits the mean of the aggregated points as a single point.
func (r *UnsignedMeanReducer) Emit() []FloatPoint {
	return []FloatPoint{{
		Time:       ZeroTime,
		Value:      float64(r.sum) / float64(r.count),
		Aggregated: r.count,
	}}
}

type FloatSpreadReducer struct {
	min, max float64
	count    uint32
}

func NewFloatSpreadReducer() *FloatSpreadReducer {
	return &FloatSpreadReducer{
		min: math.Inf(1),
		max: math.Inf(-1),
	}
}

func (r *FloatSpreadReducer) AggregateFloat(p *FloatPoint) {
	r.min = math.Min(r.min, p.Value)
	r.max = math.Max(r.max, p.Value)
	r.count++
}

func (r *FloatSpreadReducer) Emit() []FloatPoint {
	return []FloatPoint{{
		Time:       ZeroTime,
		Value:      r.max - r.min,
		Aggregated: r.count,
	}}
}

type IntegerSpreadReducer struct {
	min, max int64
	count    uint32
}

func NewIntegerSpreadReducer() *IntegerSpreadReducer {
	return &IntegerSpreadReducer{
		min: math.MaxInt64,
		max: math.MinInt64,
	}
}

func (r *IntegerSpreadReducer) AggregateInteger(p *IntegerPoint) {
	if p.Value < r.min {
		r.min = p.Value
	}
	if p.Value > r.max {
		r.max = p.Value
	}
	r.count++
}

func (r *IntegerSpreadReducer) Emit() []IntegerPoint {
	return []IntegerPoint{{
		Time:       ZeroTime,
		Value:      r.max - r.min,
		Aggregated: r.count,
	}}
}

type UnsignedSpreadReducer struct {
	min, max uint64
	count    uint32
}

func NewUnsignedSpreadReducer() *UnsignedSpreadReducer {
	return &UnsignedSpreadReducer{
		min: math.MaxUint64,
		max: 0,
	}
}

func (r *UnsignedSpreadReducer) AggregateUnsigned(p *UnsignedPoint) {
	if p.Value < r.min {
		r.min = p.Value
	}
	if p.Value > r.max {
		r.max = p.Value
	}
	r.count++
}

func (r *UnsignedSpreadReducer) Emit() []UnsignedPoint {
	return []UnsignedPoint{{
		Time:       ZeroTime,
		Value:      r.max - r.min,
		Aggregated: r.count,
	}}
}

// FloatDerivativeReducer calculates the derivative of the aggregated points.
type FloatDerivativeReducer struct {
	interval      Interval
	prev          FloatPoint
	curr          FloatPoint
	isNonNegative bool
	ascending     bool
}

// NewFloatDerivativeReducer creates a new FloatDerivativeReducer.
func NewFloatDerivativeReducer(interval Interval, isNonNegative, ascending bool) *FloatDerivativeReducer {
	return &FloatDerivativeReducer{
		interval:      interval,
		isNonNegative: isNonNegative,
		ascending:     ascending,
		prev:          FloatPoint{Nil: true},
		curr:          FloatPoint{Nil: true},
	}
}

// AggregateFloat aggregates a point into the reducer and updates the current window.
func (r *FloatDerivativeReducer) AggregateFloat(p *FloatPoint) {
	// Skip past a point when it does not advance the stream. A joined series
	// may have multiple points at the same time so we will discard anything
	// except the first point we encounter.
	if !r.curr.Nil && r.curr.Time == p.Time {
		return
	}

	r.prev = r.curr
	r.curr = *p
}

// Emit emits the derivative of the reducer at the current point.
func (r *FloatDerivativeReducer) Emit() []FloatPoint {
	if r.prev.Nil {
		return nil
	}
	// Calculate the derivative of successive points by dividing the
	// difference of each value by the elapsed time normalized to the interval.
	diff := r.curr.Value - r.prev.Value
	elapsed := r.curr.Time - r.prev.Time
	if !r.ascending {
		elapsed = -elapsed
	}
	value := diff / (float64(elapsed) / float64(r.interval.Duration))

	// Mark this point as read by changing the previous point to nil.
	r.prev.Nil = true

	// Drop negative values for non-negative derivatives.
	if r.isNonNegative && diff < 0 {
		return nil
	}
	return []FloatPoint{{Time: r.curr.Time, Value: value}}
}

// IntegerDerivativeReducer calculates the derivative of the aggregated points.
type IntegerDerivativeReducer struct {
	interval      Interval
	prev          IntegerPoint
	curr          IntegerPoint
	isNonNegative bool
	ascending     bool
}

// NewIntegerDerivativeReducer creates a new IntegerDerivativeReducer.
func NewIntegerDerivativeReducer(interval Interval, isNonNegative, ascending bool) *IntegerDerivativeReducer {
	return &IntegerDerivativeReducer{
		interval:      interval,
		isNonNegative: isNonNegative,
		ascending:     ascending,
		prev:          IntegerPoint{Nil: true},
		curr:          IntegerPoint{Nil: true},
	}
}

// AggregateInteger aggregates a point into the reducer and updates the current window.
func (r *IntegerDerivativeReducer) AggregateInteger(p *IntegerPoint) {
	// Skip past a point when it does not advance the stream. A joined series
	// may have multiple points at the same time so we will discard anything
	// except the first point we encounter.
	if !r.curr.Nil && r.curr.Time == p.Time {
		return
	}

	r.prev = r.curr
	r.curr = *p
}

// Emit emits the derivative of the reducer at the current point.
func (r *IntegerDerivativeReducer) Emit() []FloatPoint {
	if r.prev.Nil {
		return nil
	}
	// Calculate the derivative of successive points by dividing the
	// difference of each value by the elapsed time normalized to the interval.
	diff := float64(r.curr.Value - r.prev.Value)
	elapsed := r.curr.Time - r.prev.Time
	if !r.ascending {
		elapsed = -elapsed
	}
	value := diff / (float64(elapsed) / float64(r.interval.Duration))

	// Mark this point as read by changing the previous point to nil.
	r.prev.Nil = true

	// Drop negative values for non-negative derivatives.
	if r.isNonNegative && diff < 0 {
		return nil
	}
	return []FloatPoint{{Time: r.curr.Time, Value: value}}
}

// UnsignedDerivativeReducer calculates the derivative of the aggregated points.
type UnsignedDerivativeReducer struct {
	interval      Interval
	prev          UnsignedPoint
	curr          UnsignedPoint
	isNonNegative bool
	ascending     bool
}

// NewUnsignedDerivativeReducer creates a new UnsignedDerivativeReducer.
func NewUnsignedDerivativeReducer(interval Interval, isNonNegative, ascending bool) *UnsignedDerivativeReducer {
	return &UnsignedDerivativeReducer{
		interval:      interval,
		isNonNegative: isNonNegative,
		ascending:     ascending,
		prev:          UnsignedPoint{Nil: true},
		curr:          UnsignedPoint{Nil: true},
	}
}

// AggregateUnsigned aggregates a point into the reducer and updates the current window.
func (r *UnsignedDerivativeReducer) AggregateUnsigned(p *UnsignedPoint) {
	// Skip past a point when it does not advance the stream. A joined series
	// may have multiple points at the same time so we will discard anything
	// except the first point we encounter.
	if !r.curr.Nil && r.curr.Time == p.Time {
		return
	}

	r.prev = r.curr
	r.curr = *p
}

// Emit emits the derivative of the reducer at the current point.
func (r *UnsignedDerivativeReducer) Emit() []FloatPoint {
	if r.prev.Nil {
		return nil
	}
	// Calculate the derivative of successive points by dividing the
	// difference of each value by the elapsed time normalized to the interval.
	var diff float64
	if r.curr.Value > r.prev.Value {
		diff = float64(r.curr.Value - r.prev.Value)
	} else {
		diff = -float64(r.prev.Value - r.curr.Value)
	}
	elapsed := r.curr.Time - r.prev.Time
	if !r.ascending {
		elapsed = -elapsed
	}
	value := diff / (float64(elapsed) / float64(r.interval.Duration))

	// Mark this point as read by changing the previous point to nil.
	r.prev.Nil = true

	// Drop negative values for non-negative derivatives.
	if r.isNonNegative && diff < 0 {
		return nil
	}
	return []FloatPoint{{Time: r.curr.Time, Value: value}}
}

// FloatDifferenceReducer calculates the derivative of the aggregated points.
type FloatDifferenceReducer struct {
	isNonNegative bool
	prev          FloatPoint
	curr          FloatPoint
}

// NewFloatDifferenceReducer creates a new FloatDifferenceReducer.
func NewFloatDifferenceReducer(isNonNegative bool) *FloatDifferenceReducer {
	return &FloatDifferenceReducer{
		isNonNegative: isNonNegative,
		prev:          FloatPoint{Nil: true},
		curr:          FloatPoint{Nil: true},
	}
}

// AggregateFloat aggregates a point into the reducer and updates the current window.
func (r *FloatDifferenceReducer) AggregateFloat(p *FloatPoint) {
	// Skip past a point when it does not advance the stream. A joined series
	// may have multiple points at the same time so we will discard anything
	// except the first point we encounter.
	if !r.curr.Nil && r.curr.Time == p.Time {
		return
	}

	r.prev = r.curr
	r.curr = *p
}

// Emit emits the difference of the reducer at the current point.
func (r *FloatDifferenceReducer) Emit() []FloatPoint {
	if r.prev.Nil {
		return nil
	}
	// Calculate the difference of successive points.
	value := r.curr.Value - r.prev.Value

	// If it is non_negative_difference discard any negative value. Since
	// prev is still marked as unread. The correctness can be ensured.
	if r.isNonNegative && value < 0 {
		return nil
	}

	// Mark this point as read by changing the previous point to nil.
	r.prev.Nil = true
	return []FloatPoint{{Time: r.curr.Time, Value: value}}
}

// IntegerDifferenceReducer calculates the derivative of the aggregated points.
type IntegerDifferenceReducer struct {
	isNonNegative bool
	prev          IntegerPoint
	curr          IntegerPoint
}

// NewIntegerDifferenceReducer creates a new IntegerDifferenceReducer.
func NewIntegerDifferenceReducer(isNonNegative bool) *IntegerDifferenceReducer {
	return &IntegerDifferenceReducer{
		isNonNegative: isNonNegative,
		prev:          IntegerPoint{Nil: true},
		curr:          IntegerPoint{Nil: true},
	}
}

// AggregateInteger aggregates a point into the reducer and updates the current window.
func (r *IntegerDifferenceReducer) AggregateInteger(p *IntegerPoint) {
	// Skip past a point when it does not advance the stream. A joined series
	// may have multiple points at the same time so we will discard anything
	// except the first point we encounter.
	if !r.curr.Nil && r.curr.Time == p.Time {
		return
	}

	r.prev = r.curr
	r.curr = *p
}

// Emit emits the difference of the reducer at the current point.
func (r *IntegerDifferenceReducer) Emit() []IntegerPoint {
	if r.prev.Nil {
		return nil
	}
	// Calculate the difference of successive points.
	value := r.curr.Value - r.prev.Value

	// If it is non_negative_difference discard any negative value. Since
	// prev is still marked as unread. The correctness can be ensured.
	if r.isNonNegative && value < 0 {
		return nil
	}

	// Mark this point as read by changing the previous point to nil.
	r.prev.Nil = true

	return []IntegerPoint{{Time: r.curr.Time, Value: value}}
}

// UnsignedDifferenceReducer calculates the derivative of the aggregated points.
type UnsignedDifferenceReducer struct {
	isNonNegative bool
	prev          UnsignedPoint
	curr          UnsignedPoint
}

// NewUnsignedDifferenceReducer creates a new UnsignedDifferenceReducer.
func NewUnsignedDifferenceReducer(isNonNegative bool) *UnsignedDifferenceReducer {
	return &UnsignedDifferenceReducer{
		isNonNegative: isNonNegative,
		prev:          UnsignedPoint{Nil: true},
		curr:          UnsignedPoint{Nil: true},
	}
}

// AggregateUnsigned aggregates a point into the reducer and updates the current window.
func (r *UnsignedDifferenceReducer) AggregateUnsigned(p *UnsignedPoint) {
	// Skip past a point when it does not advance the stream. A joined series
	// may have multiple points at the same time so we will discard anything
	// except the first point we encounter.
	if !r.curr.Nil && r.curr.Time == p.Time {
		return
	}

	r.prev = r.curr
	r.curr = *p
}

// Emit emits the difference of the reducer at the current point.
func (r *UnsignedDifferenceReducer) Emit() []UnsignedPoint {
	if r.prev.Nil {
		return nil
	}
	// If it is non_negative_difference discard any negative value. Since
	// prev is still marked as unread. The correctness can be ensured.
	if r.isNonNegative && r.curr.Value < r.prev.Value {
		return nil
	}

	// Calculate the difference of successive points.
	value := r.curr.Value - r.prev.Value

	// Mark this point as read by changing the previous point to nil.
	r.prev.Nil = true

	return []UnsignedPoint{{Time: r.curr.Time, Value: value}}
}

// FloatMovingAverageReducer calculates the moving average of the aggregated points.
type FloatMovingAverageReducer struct {
	pos  int
	sum  float64
	time int64
	buf  []float64
}

// NewFloatMovingAverageReducer creates a new FloatMovingAverageReducer.
func NewFloatMovingAverageReducer(n int) *FloatMovingAverageReducer {
	return &FloatMovingAverageReducer{
		buf: make([]float64, 0, n),
	}
}

// AggregateFloat aggregates a point into the reducer and updates the current window.
func (r *FloatMovingAverageReducer) AggregateFloat(p *FloatPoint) {
	if len(r.buf) != cap(r.buf) {
		r.buf = append(r.buf, p.Value)
	} else {
		r.sum -= r.buf[r.pos]
		r.buf[r.pos] = p.Value
	}
	r.sum += p.Value
	r.time = p.Time
	r.pos++
	if r.pos >= cap(r.buf) {
		r.pos = 0
	}
}

// Emit emits the moving average of the current window. Emit should be called
// after every call to AggregateFloat and it will produce one point if there
// is enough data to fill a window, otherwise it will produce zero points.
func (r *FloatMovingAverageReducer) Emit() []FloatPoint {
	if len(r.buf) != cap(r.buf) {
		return []FloatPoint{}
	}
	return []FloatPoint{
		{
			Value:      r.sum / float64(len(r.buf)),
			Time:       r.time,
			Aggregated: uint32(len(r.buf)),
		},
	}
}

// IntegerMovingAverageReducer calculates the moving average of the aggregated points.
type IntegerMovingAverageReducer struct {
	pos  int
	sum  int64
	time int64
	buf  []int64
}

// NewIntegerMovingAverageReducer creates a new IntegerMovingAverageReducer.
func NewIntegerMovingAverageReducer(n int) *IntegerMovingAverageReducer {
	return &IntegerMovingAverageReducer{
		buf: make([]int64, 0, n),
	}
}

// AggregateInteger aggregates a point into the reducer and updates the current window.
func (r *IntegerMovingAverageReducer) AggregateInteger(p *IntegerPoint) {
	if len(r.buf) != cap(r.buf) {
		r.buf = append(r.buf, p.Value)
	} else {
		r.sum -= r.buf[r.pos]
		r.buf[r.pos] = p.Value
	}
	r.sum += p.Value
	r.time = p.Time
	r.pos++
	if r.pos >= cap(r.buf) {
		r.pos = 0
	}
}

// Emit emits the moving average of the current window. Emit should be called
// after every call to AggregateInteger and it will produce one point if there
// is enough data to fill a window, otherwise it will produce zero points.
func (r *IntegerMovingAverageReducer) Emit() []FloatPoint {
	if len(r.buf) != cap(r.buf) {
		return []FloatPoint{}
	}
	return []FloatPoint{
		{
			Value:      float64(r.sum) / float64(len(r.buf)),
			Time:       r.time,
			Aggregated: uint32(len(r.buf)),
		},
	}
}

// UnsignedMovingAverageReducer calculates the moving average of the aggregated points.
type UnsignedMovingAverageReducer struct {
	pos  int
	sum  uint64
	time int64
	buf  []uint64
}

// NewUnsignedMovingAverageReducer creates a new UnsignedMovingAverageReducer.
func NewUnsignedMovingAverageReducer(n int) *UnsignedMovingAverageReducer {
	return &UnsignedMovingAverageReducer{
		buf: make([]uint64, 0, n),
	}
}

// AggregateUnsigned aggregates a point into the reducer and updates the current window.
func (r *UnsignedMovingAverageReducer) AggregateUnsigned(p *UnsignedPoint) {
	if len(r.buf) != cap(r.buf) {
		r.buf = append(r.buf, p.Value)
	} else {
		r.sum -= r.buf[r.pos]
		r.buf[r.pos] = p.Value
	}
	r.sum += p.Value
	r.time = p.Time
	r.pos++
	if r.pos >= cap(r.buf) {
		r.pos = 0
	}
}

// Emit emits the moving average of the current window. Emit should be called
// after every call to AggregateUnsigned and it will produce one point if there
// is enough data to fill a window, otherwise it will produce zero points.
func (r *UnsignedMovingAverageReducer) Emit() []FloatPoint {
	if len(r.buf) != cap(r.buf) {
		return []FloatPoint{}
	}
	return []FloatPoint{
		{
			Value:      float64(r.sum) / float64(len(r.buf)),
			Time:       r.time,
			Aggregated: uint32(len(r.buf)),
		},
	}
}

// FloatCumulativeSumReducer cumulates the values from each point.
type FloatCumulativeSumReducer struct {
	curr FloatPoint
}

// NewFloatCumulativeSumReducer creates a new FloatCumulativeSumReducer.
func NewFloatCumulativeSumReducer() *FloatCumulativeSumReducer {
	return &FloatCumulativeSumReducer{
		curr: FloatPoint{Nil: true},
	}
}

func (r *FloatCumulativeSumReducer) AggregateFloat(p *FloatPoint) {
	r.curr.Value += p.Value
	r.curr.Time = p.Time
	r.curr.Nil = false
}

func (r *FloatCumulativeSumReducer) Emit() []FloatPoint {
	var pts []FloatPoint
	if !r.curr.Nil {
		pts = []FloatPoint{r.curr}
	}
	return pts
}

// IntegerCumulativeSumReducer cumulates the values from each point.
type IntegerCumulativeSumReducer struct {
	curr IntegerPoint
}

// NewIntegerCumulativeSumReducer creates a new IntegerCumulativeSumReducer.
func NewIntegerCumulativeSumReducer() *IntegerCumulativeSumReducer {
	return &IntegerCumulativeSumReducer{
		curr: IntegerPoint{Nil: true},
	}
}

func (r *IntegerCumulativeSumReducer) AggregateInteger(p *IntegerPoint) {
	r.curr.Value += p.Value
	r.curr.Time = p.Time
	r.curr.Nil = false
}

func (r *IntegerCumulativeSumReducer) Emit() []IntegerPoint {
	var pts []IntegerPoint
	if !r.curr.Nil {
		pts = []IntegerPoint{r.curr}
	}
	return pts
}

// UnsignedCumulativeSumReducer cumulates the values from each point.
type UnsignedCumulativeSumReducer struct {
	curr UnsignedPoint
}

// NewUnsignedCumulativeSumReducer creates a new UnsignedCumulativeSumReducer.
func NewUnsignedCumulativeSumReducer() *UnsignedCumulativeSumReducer {
	return &UnsignedCumulativeSumReducer{
		curr: UnsignedPoint{Nil: true},
	}
}

func (r *UnsignedCumulativeSumReducer) AggregateUnsigned(p *UnsignedPoint) {
	r.curr.Value += p.Value
	r.curr.Time = p.Time
	r.curr.Nil = false
}

func (r *UnsignedCumulativeSumReducer) Emit() []UnsignedPoint {
	var pts []UnsignedPoint
	if !r.curr.Nil {
		pts = []UnsignedPoint{r.curr}
	}
	return pts
}

// FloatIntegralReducer calculates the time-integral of the aggregated points.
type FloatIntegralReducer struct {
	interval Interval
	sum      float64
	prev     FloatPoint
	window   struct {
		start int64
		end   int64
	}
	ch  chan FloatPoint
	opt IteratorOptions
}

// NewFloatIntegralReducer creates a new FloatIntegralReducer.
func NewFloatIntegralReducer(interval Interval, opt IteratorOptions) *FloatIntegralReducer {
	return &FloatIntegralReducer{
		interval: interval,
		prev:     FloatPoint{Nil: true},
		ch:       make(chan FloatPoint, 1),
		opt:      opt,
	}
}

// AggregateFloat aggregates a point into the reducer.
func (r *FloatIntegralReducer) AggregateFloat(p *FloatPoint) {
	// If this is the first point, just save it
	if r.prev.Nil {
		r.prev = *p
		if !r.opt.Interval.IsZero() {
			// Record the end of the time interval.
			// We do not care for whether the last number is inclusive or exclusive
			// because we treat both the same for the involved math.
			if r.opt.Ascending {
				r.window.start, r.window.end = r.opt.Window(p.Time)
			} else {
				r.window.end, r.window.start = r.opt.Window(p.Time)
			}
		}
		return
	}

	// If this point has the same timestamp as the previous one,
	// skip the point. Points sent into this reducer are expected
	// to be fed in order.
	if r.prev.Time == p.Time {
		r.prev = *p
		return
	} else if !r.opt.Interval.IsZero() && ((r.opt.Ascending && p.Time >= r.window.end) || (!r.opt.Ascending && p.Time <= r.window.end)) {
		// If our previous time is not equal to the window, we need to
		// interpolate the area at the end of this interval.
		if r.prev.Time != r.window.end {
			value := linearFloat(r.window.end, r.prev.Time, p.Time, r.prev.Value, p.Value)
			elapsed := float64(r.window.end-r.prev.Time) / float64(r.interval.Duration)
			r.sum += 0.5 * (value + r.prev.Value) * elapsed

			r.prev.Value = value
			r.prev.Time = r.window.end
		}

		// Emit the current point through the channel and then clear it.
		r.ch <- FloatPoint{Time: r.window.start, Value: r.sum}
		if r.opt.Ascending {
			r.window.start, r.window.end = r.opt.Window(p.Time)
		} else {
			r.window.end, r.window.start = r.opt.Window(p.Time)
		}
		r.sum = 0.0
	}

	// Normal operation: update the sum using the trapezium rule
	elapsed := float64(p.Time-r.prev.Time) / float64(r.interval.Duration)
	r.sum += 0.5 * (p.Value + r.prev.Value) * elapsed
	r.prev = *p
}

// Emit emits the time-integral of the aggregated points as a single point.
// CnosQL convention dictates that outside a group-by-time clause we return
// a timestamp of zero.  Within a group-by-time, we can set the time to ZeroTime
// and a higher level will change it to the start of the time group.
func (r *FloatIntegralReducer) Emit() []FloatPoint {
	select {
	case pt, ok := <-r.ch:
		if !ok {
			return nil
		}
		return []FloatPoint{pt}
	default:
		return nil
	}
}

// Close flushes any in progress points to ensure any remaining points are
// emitted.
func (r *FloatIntegralReducer) Close() error {
	// If our last point is at the start time, then discard this point since
	// there is no area within this bucket. Otherwise, send off what we
	// currently have as the final point.
	if !r.prev.Nil && r.prev.Time != r.window.start {
		r.ch <- FloatPoint{Time: r.window.start, Value: r.sum}
	}
	close(r.ch)
	return nil
}

// IntegerIntegralReducer calculates the time-integral of the aggregated points.
type IntegerIntegralReducer struct {
	interval Interval
	sum      float64
	prev     IntegerPoint
	window   struct {
		start int64
		end   int64
	}
	ch  chan FloatPoint
	opt IteratorOptions
}

// NewIntegerIntegralReducer creates a new IntegerIntegralReducer.
func NewIntegerIntegralReducer(interval Interval, opt IteratorOptions) *IntegerIntegralReducer {
	return &IntegerIntegralReducer{
		interval: interval,
		prev:     IntegerPoint{Nil: true},
		ch:       make(chan FloatPoint, 1),
		opt:      opt,
	}
}

// AggregateInteger aggregates a point into the reducer.
func (r *IntegerIntegralReducer) AggregateInteger(p *IntegerPoint) {
	// If this is the first point, just save it
	if r.prev.Nil {
		r.prev = *p

		// Record the end of the time interval.
		// We do not care for whether the last number is inclusive or exclusive
		// because we treat both the same for the involved math.
		if r.opt.Ascending {
			r.window.start, r.window.end = r.opt.Window(p.Time)
		} else {
			r.window.end, r.window.start = r.opt.Window(p.Time)
		}

		// If we see the minimum allowable time, set the time to zero so we don't
		// break the default returned time for aggregate queries without times.
		if r.window.start == cnosql.MinTime {
			r.window.start = 0
		}
		return
	}

	// If this point has the same timestamp as the previous one,
	// skip the point. Points sent into this reducer are expected
	// to be fed in order.
	value := float64(p.Value)
	if r.prev.Time == p.Time {
		r.prev = *p
		return
	} else if (r.opt.Ascending && p.Time >= r.window.end) || (!r.opt.Ascending && p.Time <= r.window.end) {
		// If our previous time is not equal to the window, we need to
		// interpolate the area at the end of this interval.
		if r.prev.Time != r.window.end {
			value = linearFloat(r.window.end, r.prev.Time, p.Time, float64(r.prev.Value), value)
			elapsed := float64(r.window.end-r.prev.Time) / float64(r.interval.Duration)
			r.sum += 0.5 * (value + float64(r.prev.Value)) * elapsed

			r.prev.Time = r.window.end
		}

		// Emit the current point through the channel and then clear it.
		r.ch <- FloatPoint{Time: r.window.start, Value: r.sum}
		if r.opt.Ascending {
			r.window.start, r.window.end = r.opt.Window(p.Time)
		} else {
			r.window.end, r.window.start = r.opt.Window(p.Time)
		}
		r.sum = 0.0
	}

	// Normal operation: update the sum using the trapezium rule
	elapsed := float64(p.Time-r.prev.Time) / float64(r.interval.Duration)
	r.sum += 0.5 * (value + float64(r.prev.Value)) * elapsed
	r.prev = *p
}

// Emit emits the time-integral of the aggregated points as a single FLOAT point
// CnosQL convention dictates that outside a group-by-time clause we return
// a timestamp of zero.  Within a group-by-time, we can set the time to ZeroTime
// and a higher level will change it to the start of the time group.
func (r *IntegerIntegralReducer) Emit() []FloatPoint {
	select {
	case pt, ok := <-r.ch:
		if !ok {
			return nil
		}
		return []FloatPoint{pt}
	default:
		return nil
	}
}

// Close flushes any in progress points to ensure any remaining points are
// emitted.
func (r *IntegerIntegralReducer) Close() error {
	// If our last point is at the start time, then discard this point since
	// there is no area within this bucket. Otherwise, send off what we
	// currently have as the final point.
	if !r.prev.Nil && r.prev.Time != r.window.start {
		r.ch <- FloatPoint{Time: r.window.start, Value: r.sum}
	}
	close(r.ch)
	return nil
}

// IntegerIntegralReducer calculates the time-integral of the aggregated points.
type UnsignedIntegralReducer struct {
	interval Interval
	sum      float64
	prev     UnsignedPoint
	window   struct {
		start int64
		end   int64
	}
	ch  chan FloatPoint
	opt IteratorOptions
}

// NewUnsignedIntegralReducer creates a new UnsignedIntegralReducer.
func NewUnsignedIntegralReducer(interval Interval, opt IteratorOptions) *UnsignedIntegralReducer {
	return &UnsignedIntegralReducer{
		interval: interval,
		prev:     UnsignedPoint{Nil: true},
		ch:       make(chan FloatPoint, 1),
		opt:      opt,
	}
}

// AggregateUnsigned aggregates a point into the reducer.
func (r *UnsignedIntegralReducer) AggregateUnsigned(p *UnsignedPoint) {
	// If this is the first point, just save it
	if r.prev.Nil {
		r.prev = *p

		// Record the end of the time interval.
		// We do not care for whether the last number is inclusive or exclusive
		// because we treat both the same for the involved math.
		if r.opt.Ascending {
			r.window.start, r.window.end = r.opt.Window(p.Time)
		} else {
			r.window.end, r.window.start = r.opt.Window(p.Time)
		}

		// If we see the minimum allowable time, set the time to zero so we don't
		// break the default returned time for aggregate queries without times.
		if r.window.start == cnosql.MinTime {
			r.window.start = 0
		}
		return
	}

	// If this point has the same timestamp as the previous one,
	// skip the point. Points sent into this reducer are expected
	// to be fed in order.
	value := float64(p.Value)
	if r.prev.Time == p.Time {
		r.prev = *p
		return
	} else if (r.opt.Ascending && p.Time >= r.window.end) || (!r.opt.Ascending && p.Time <= r.window.end) {
		// If our previous time is not equal to the window, we need to
		// interpolate the area at the end of this interval.
		if r.prev.Time != r.window.end {
			value = linearFloat(r.window.end, r.prev.Time, p.Time, float64(r.prev.Value), value)
			elapsed := float64(r.window.end-r.prev.Time) / float64(r.interval.Duration)
			r.sum += 0.5 * (value + float64(r.prev.Value)) * elapsed

			r.prev.Time = r.window.end
		}

		// Emit the current point through the channel and then clear it.
		r.ch <- FloatPoint{Time: r.window.start, Value: r.sum}
		if r.opt.Ascending {
			r.window.start, r.window.end = r.opt.Window(p.Time)
		} else {
			r.window.end, r.window.start = r.opt.Window(p.Time)
		}
		r.sum = 0.0
	}

	// Normal operation: update the sum using the trapezium rule
	elapsed := float64(p.Time-r.prev.Time) / float64(r.interval.Duration)
	r.sum += 0.5 * (value + float64(r.prev.Value)) * elapsed
	r.prev = *p
}

// Emit emits the time-integral of the aggregated points as a single FLOAT point
// CnosQL convention dictates that outside a group-by-time clause we return
// a timestamp of zero.  Within a group-by-time, we can set the time to ZeroTime
// and a higher level will change it to the start of the time group.
func (r *UnsignedIntegralReducer) Emit() []FloatPoint {
	select {
	case pt, ok := <-r.ch:
		if !ok {
			return nil
		}
		return []FloatPoint{pt}
	default:
		return nil
	}
}

// Close flushes any in progress points to ensure any remaining points are
// emitted.
func (r *UnsignedIntegralReducer) Close() error {
	// If our last point is at the start time, then discard this point since
	// there is no area within this bucket. Otherwise, send off what we
	// currently have as the final point.
	if !r.prev.Nil && r.prev.Time != r.window.start {
		r.ch <- FloatPoint{Time: r.window.start, Value: r.sum}
	}
	close(r.ch)
	return nil
}

type FloatTopReducer struct {
	h *floatPointsByFunc
}

func NewFloatTopReducer(n int) *FloatTopReducer {
	return &FloatTopReducer{
		h: floatPointsSortBy(make([]FloatPoint, 0, n), func(a, b *FloatPoint) bool {
			if a.Value != b.Value {
				return a.Value < b.Value
			}
			return a.Time > b.Time
		}),
	}
}

func (r *FloatTopReducer) AggregateFloat(p *FloatPoint) {
	if r.h.Len() == cap(r.h.points) {
		// Compare the minimum point and the aggregated point. If our value is
		// larger, replace the current min value.
		if !r.h.cmp(&r.h.points[0], p) {
			return
		}
		p.CopyTo(&r.h.points[0])
		heap.Fix(r.h, 0)
		return
	}

	var clone FloatPoint
	p.CopyTo(&clone)
	heap.Push(r.h, clone)
}

func (r *FloatTopReducer) Emit() []FloatPoint {
	// Ensure the points are sorted with the maximum value last. While the
	// first point may be the minimum value, the rest is not guaranteed to be
	// in any particular order while it is a heap.
	points := make([]FloatPoint, len(r.h.points))
	for i, p := range r.h.points {
		p.Aggregated = 0
		points[i] = p
	}
	h := floatPointsByFunc{points: points, cmp: r.h.cmp}
	sort.Sort(sort.Reverse(&h))
	return points
}

type IntegerTopReducer struct {
	h *integerPointsByFunc
}

func NewIntegerTopReducer(n int) *IntegerTopReducer {
	return &IntegerTopReducer{
		h: integerPointsSortBy(make([]IntegerPoint, 0, n), func(a, b *IntegerPoint) bool {
			if a.Value != b.Value {
				return a.Value < b.Value
			}
			return a.Time > b.Time
		}),
	}
}

func (r *IntegerTopReducer) AggregateInteger(p *IntegerPoint) {
	if r.h.Len() == cap(r.h.points) {
		// Compare the minimum point and the aggregated point. If our value is
		// larger, replace the current min value.
		if !r.h.cmp(&r.h.points[0], p) {
			return
		}
		p.CopyTo(&r.h.points[0])
		heap.Fix(r.h, 0)
		return
	}

	var clone IntegerPoint
	p.CopyTo(&clone)
	heap.Push(r.h, clone)
}

func (r *IntegerTopReducer) Emit() []IntegerPoint {
	// Ensure the points are sorted with the maximum value last. While the
	// first point may be the minimum value, the rest is not guaranteed to be
	// in any particular order while it is a heap.
	points := make([]IntegerPoint, len(r.h.points))
	for i, p := range r.h.points {
		p.Aggregated = 0
		points[i] = p
	}
	h := integerPointsByFunc{points: points, cmp: r.h.cmp}
	sort.Sort(sort.Reverse(&h))
	return points
}

type UnsignedTopReducer struct {
	h *unsignedPointsByFunc
}

func NewUnsignedTopReducer(n int) *UnsignedTopReducer {
	return &UnsignedTopReducer{
		h: unsignedPointsSortBy(make([]UnsignedPoint, 0, n), func(a, b *UnsignedPoint) bool {
			if a.Value != b.Value {
				return a.Value < b.Value
			}
			return a.Time > b.Time
		}),
	}
}

func (r *UnsignedTopReducer) AggregateUnsigned(p *UnsignedPoint) {
	if r.h.Len() == cap(r.h.points) {
		// Compare the minimum point and the aggregated point. If our value is
		// larger, replace the current min value.
		if !r.h.cmp(&r.h.points[0], p) {
			return
		}
		p.CopyTo(&r.h.points[0])
		heap.Fix(r.h, 0)
		return
	}

	var clone UnsignedPoint
	p.CopyTo(&clone)
	heap.Push(r.h, clone)
}

func (r *UnsignedTopReducer) Emit() []UnsignedPoint {
	// Ensure the points are sorted with the maximum value last. While the
	// first point may be the minimum value, the rest is not guaranteed to be
	// in any particular order while it is a heap.
	points := make([]UnsignedPoint, len(r.h.points))
	for i, p := range r.h.points {
		p.Aggregated = 0
		points[i] = p
	}
	h := unsignedPointsByFunc{points: points, cmp: r.h.cmp}
	sort.Sort(sort.Reverse(&h))
	return points
}

type FloatBottomReducer struct {
	h *floatPointsByFunc
}

func NewFloatBottomReducer(n int) *FloatBottomReducer {
	return &FloatBottomReducer{
		h: floatPointsSortBy(make([]FloatPoint, 0, n), func(a, b *FloatPoint) bool {
			if a.Value != b.Value {
				return a.Value > b.Value
			}
			return a.Time > b.Time
		}),
	}
}

func (r *FloatBottomReducer) AggregateFloat(p *FloatPoint) {
	if r.h.Len() == cap(r.h.points) {
		// Compare the minimum point and the aggregated point. If our value is
		// larger, replace the current min value.
		if !r.h.cmp(&r.h.points[0], p) {
			return
		}
		p.CopyTo(&r.h.points[0])
		heap.Fix(r.h, 0)
		return
	}

	var clone FloatPoint
	p.CopyTo(&clone)
	heap.Push(r.h, clone)
}

func (r *FloatBottomReducer) Emit() []FloatPoint {
	// Ensure the points are sorted with the maximum value last. While the
	// first point may be the minimum value, the rest is not guaranteed to be
	// in any particular order while it is a heap.
	points := make([]FloatPoint, len(r.h.points))
	for i, p := range r.h.points {
		p.Aggregated = 0
		points[i] = p
	}
	h := floatPointsByFunc{points: points, cmp: r.h.cmp}
	sort.Sort(sort.Reverse(&h))
	return points
}

type IntegerBottomReducer struct {
	h *integerPointsByFunc
}

func NewIntegerBottomReducer(n int) *IntegerBottomReducer {
	return &IntegerBottomReducer{
		h: integerPointsSortBy(make([]IntegerPoint, 0, n), func(a, b *IntegerPoint) bool {
			if a.Value != b.Value {
				return a.Value > b.Value
			}
			return a.Time > b.Time
		}),
	}
}

func (r *IntegerBottomReducer) AggregateInteger(p *IntegerPoint) {
	if r.h.Len() == cap(r.h.points) {
		// Compare the minimum point and the aggregated point. If our value is
		// larger, replace the current min value.
		if !r.h.cmp(&r.h.points[0], p) {
			return
		}
		p.CopyTo(&r.h.points[0])
		heap.Fix(r.h, 0)
		return
	}

	var clone IntegerPoint
	p.CopyTo(&clone)
	heap.Push(r.h, clone)
}

func (r *IntegerBottomReducer) Emit() []IntegerPoint {
	// Ensure the points are sorted with the maximum value last. While the
	// first point may be the minimum value, the rest is not guaranteed to be
	// in any particular order while it is a heap.
	points := make([]IntegerPoint, len(r.h.points))
	for i, p := range r.h.points {
		p.Aggregated = 0
		points[i] = p
	}
	h := integerPointsByFunc{points: points, cmp: r.h.cmp}
	sort.Sort(sort.Reverse(&h))
	return points
}

type UnsignedBottomReducer struct {
	h *unsignedPointsByFunc
}

func NewUnsignedBottomReducer(n int) *UnsignedBottomReducer {
	return &UnsignedBottomReducer{
		h: unsignedPointsSortBy(make([]UnsignedPoint, 0, n), func(a, b *UnsignedPoint) bool {
			if a.Value != b.Value {
				return a.Value > b.Value
			}
			return a.Time > b.Time
		}),
	}
}

func (r *UnsignedBottomReducer) AggregateUnsigned(p *UnsignedPoint) {
	if r.h.Len() == cap(r.h.points) {
		// Compare the minimum point and the aggregated point. If our value is
		// larger, replace the current min value.
		if !r.h.cmp(&r.h.points[0], p) {
			return
		}
		p.CopyTo(&r.h.points[0])
		heap.Fix(r.h, 0)
		return
	}

	var clone UnsignedPoint
	p.CopyTo(&clone)
	heap.Push(r.h, clone)
}

func (r *UnsignedBottomReducer) Emit() []UnsignedPoint {
	// Ensure the points are sorted with the maximum value last. While the
	// first point may be the minimum value, the rest is not guaranteed to be
	// in any particular order while it is a heap.
	points := make([]UnsignedPoint, len(r.h.points))
	for i, p := range r.h.points {
		p.Aggregated = 0
		points[i] = p
	}
	h := unsignedPointsByFunc{points: points, cmp: r.h.cmp}
	sort.Sort(sort.Reverse(&h))
	return points
}
