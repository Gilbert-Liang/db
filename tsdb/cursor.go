package tsdb

import (
	"db/tsdb/cursors"

	"db/query"
)

type (
	IntegerArray  = cursors.IntegerArray
	FloatArray    = cursors.FloatArray
	UnsignedArray = cursors.UnsignedArray
	StringArray   = cursors.StringArray
	BooleanArray  = cursors.BooleanArray

	IntegerArrayCursor  = cursors.IntegerArrayCursor
	FloatArrayCursor    = cursors.FloatArrayCursor
	UnsignedArrayCursor = cursors.UnsignedArrayCursor
	StringArrayCursor   = cursors.StringArrayCursor
	BooleanArrayCursor  = cursors.BooleanArrayCursor

	Cursor          = cursors.Cursor
	CursorStats     = cursors.CursorStats
	CursorRequest   = cursors.CursorRequest
	CursorIterator  = cursors.CursorIterator
	CursorIterators = cursors.CursorIterators
)

func NewIntegerArrayLen(sz int) *IntegerArray   { return cursors.NewIntegerArrayLen(sz) }
func NewFloatArrayLen(sz int) *FloatArray       { return cursors.NewFloatArrayLen(sz) }
func NewUnsignedArrayLen(sz int) *UnsignedArray { return cursors.NewUnsignedArrayLen(sz) }
func NewStringArrayLen(sz int) *StringArray     { return cursors.NewStringArrayLen(sz) }
func NewBooleanArrayLen(sz int) *BooleanArray   { return cursors.NewBooleanArrayLen(sz) }

// EOF represents a "not found" key returned by a Cursor.
const EOF = query.ZeroTime
