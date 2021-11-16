package internal

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"strings"
	"time"

	"db/model"
)

// Point represents a single data point.
type Point struct {
	pt model.Point
}

// NewPoint 生成一个带时间的 Point。若时间 t 未指定，则服务端会根据服务器时间设定 Point 的时间。
// 建议由客户端指定时间 t 。
func NewPoint(
	name string,
	tags map[string]string,
	fields map[string]interface{},
	t ...time.Time,
) (*Point, error) {
	var T time.Time
	if len(t) > 0 {
		T = t[0]
	}

	pt, err := model.NewPoint(name, model.NewTags(tags), fields, T)
	if err != nil {
		return nil, err
	}
	return &Point{
		pt: pt,
	}, nil
}

// NewPointFrom returns a point from the provided model.Point.
func NewPointFrom(pt model.Point) *Point {
	return &Point{pt: pt}
}

// String returns a line-protocol string of the Point.
func (p *Point) String() string {
	return p.pt.String()
}

// PrecisionString returns a line-protocol string of the Point,
// with the timestamp formatted for the given precision.
func (p *Point) PrecisionString(precision string) string {
	return p.pt.PrecisionString(precision)
}

// Name returns the metric name of the point.
func (p *Point) Name() string {
	return string(p.pt.Name())
}

// Tags returns the tags associated with the point.
func (p *Point) Tags() map[string]string {
	return p.pt.Tags().Map()
}

// Time return the timestamp for the point.
func (p *Point) Time() time.Time {
	return p.pt.Time()
}

// UnixNano returns timestamp of the point in nanoseconds since Unix epoch.
func (p *Point) UnixNano() int64 {
	return p.pt.UnixNano()
}

// Fields returns the fields for the point.
func (p *Point) Fields() (map[string]interface{}, error) {
	return p.pt.Fields()
}

type BatchPoints struct {
	Points     []*Point
	Database   string
	Precision  string
	TimeToLive string
}

// NewBatchPoints returns a BatchPoints interface based on the given config.
func NewBatchPoints(precision, database, timeToLive string) (*BatchPoints, error) {
	if precision == "" {
		precision = "ns"
	}
	if _, err := time.ParseDuration("1" + precision); err != nil {
		return nil, err
	}
	bp := &BatchPoints{
		Database:   database,
		Precision:  precision,
		TimeToLive: timeToLive,
	}
	return bp, nil
}

// Message represents a user message.
type Message struct {
	Level string
	Text  string
}

// Result represents a resultset returned from a single statement.
type Result struct {
	Series   []model.Row
	Messages []*Message
	Err      string `json:"error,omitempty"`
}

// Response represents a list of statement results.
type Response struct {
	Results []Result
	Err     string `json:"error,omitempty"`
}

// Error returns the first error from any statement.
// It returns nil if no errors occurred on any statements.
func (r *Response) Error() error {
	if r.Err != "" {
		return errors.New(r.Err)
	}
	for _, result := range r.Results {
		if result.Err != "" {
			return errors.New(result.Err)
		}
	}
	return nil
}

// duplexReader reads responses and writes it to another writer while
// satisfying the reader interface.
type duplexReader struct {
	r io.ReadCloser
	w io.Writer
}

func (r *duplexReader) Read(p []byte) (n int, err error) {
	n, err = r.r.Read(p)
	if err == nil {
		r.w.Write(p[:n])
	}
	return n, err
}

// Close closes the response.
func (r *duplexReader) Close() error {
	return r.r.Close()
}

// ChunkedResponse represents a response from the server that
// uses chunking to stream the output.
type ChunkedResponse struct {
	dec    *json.Decoder
	duplex *duplexReader
	buf    bytes.Buffer
}

// NewChunkedResponse reads a stream and produces responses from the stream.
func NewChunkedResponse(r io.Reader) *ChunkedResponse {
	rc, ok := r.(io.ReadCloser)
	if !ok {
		rc = ioutil.NopCloser(r)
	}
	resp := &ChunkedResponse{}
	resp.duplex = &duplexReader{r: rc, w: &resp.buf}
	resp.dec = json.NewDecoder(resp.duplex)
	resp.dec.UseNumber()
	return resp
}

// NextResponse reads the next line of the stream and returns a response.
func (r *ChunkedResponse) NextResponse() (*Response, error) {
	var response Response
	if err := r.dec.Decode(&response); err != nil {
		if err == io.EOF {
			return nil, err
		}
		// 解析失败，通常是服务端出现异常，返回了该异常信息。此时需要确保剩余的异常信息被读取
		_, _ = io.Copy(ioutil.Discard, r.duplex)
		return nil, errors.New(strings.TrimSpace(r.buf.String()))
	}

	r.buf.Reset()
	return &response, nil
}

// Close closes the response.
func (r *ChunkedResponse) Close() error {
	return r.duplex.Close()
}
