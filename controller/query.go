package controller

import (
	"db/config"
	"db/parser/cnosql"
	"db/query"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type QueryAPI struct {
	Config *config.HTTP

	QueryExecutor *query.Executor
}

func (h *QueryAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rw, ok := w.(ResponseWriter)
	if !ok {
		rw = NewResponseWriter(w, r)
	}

	// Request param "q"
	var qr io.Reader
	if q := strings.TrimSpace(r.FormValue("q")); q != "" {
		qr = strings.NewReader(q)
	}
	if qr == nil {
		writeError(rw, `missing required parameter "q"`)
		return
	}
	p := cnosql.NewParser(qr)

	// Request param "epoch"
	epoch := strings.TrimSpace(r.FormValue("epoch"))
	// Request param "db"
	db := strings.TrimSpace(r.FormValue("db"))
	// Request param "ttl"
	ttl := strings.TrimSpace(r.FormValue("ttl"))

	q, err := p.ParseQuery()
	if err != nil {
		writeError(rw, fmt.Sprintf("error parsing query: %s", err.Error()))
		return
	}

	opts := query.ExecutionOptions{
		Database:   db,
		TimeToLive: ttl,
		ReadOnly:   r.Method == "GET",
	}

	closeCh := make(chan struct{})
	doneCh := make(chan struct{})
	defer close(doneCh)
	go func() {
		select {
		case <-doneCh:
		case <-r.Context().Done():
			close(closeCh)
		}
	}()
	opts.AbortCh = closeCh
	// Execute the query
	results := h.QueryExecutor.ExecuteQuery(q, opts, doneCh)

	resp := Response{Results: make([]*query.Result, 0)}

	writeHeader(rw, http.StatusOK)
	rw.Flush()

	for ret := range results {
		// Ignore nil results.
		if ret == nil {
			continue
		}

		// if requested, convert result timestamps to epoch
		if epoch != "" {
			convertToEpoch(ret, epoch)
		}

		l := len(resp.Results)
		if l == 0 {
			resp.Results = append(resp.Results, ret)
		} else if resp.Results[l-1].StatementID == ret.StatementID {
			if ret.Err != nil {
				resp.Results[l-1] = ret
				continue
			}

			cr := resp.Results[l-1]
			rowsMerged := 0
			if len(cr.Series) > 0 {
				lastSeries := cr.Series[len(cr.Series)-1]

				for _, row := range ret.Series {
					if !lastSeries.SameSeries(row) {
						// Next row is for a different series than last.
						break
					}
					// Values are for the same series, so append them.
					lastSeries.Values = append(lastSeries.Values, row.Values...)
					lastSeries.Partial = row.Partial
					rowsMerged++
				}
			}

			// Append remaining rows as new rows.
			ret.Series = ret.Series[rowsMerged:]
			cr.Series = append(cr.Series, ret.Series...)
			cr.Messages = append(cr.Messages, ret.Messages...)
			cr.Partial = ret.Partial
		} else {
			resp.Results = append(resp.Results, ret)
		}
	}

	_, _ = rw.WriteResponse(resp)
}

// convertToEpoch converts result timestamps from time.Time to the specified epoch.
func convertToEpoch(r *query.Result, epoch string) {
	divisor := int64(1)

	switch epoch {
	case "u":
		divisor = int64(time.Microsecond)
	case "ms":
		divisor = int64(time.Millisecond)
	case "s":
		divisor = int64(time.Second)
	case "m":
		divisor = int64(time.Minute)
	case "h":
		divisor = int64(time.Hour)
	}

	for _, s := range r.Series {
		for _, v := range s.Values {
			if ts, ok := v[0].(time.Time); ok {
				v[0] = ts.UnixNano() / divisor
			}
		}
	}
}
