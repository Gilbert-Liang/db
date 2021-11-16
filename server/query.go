package server

import (
	"db/config"
	"db/parser/cnosql"
	"db/query"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

const (
	DefaultChunkSize = 10000
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

	// Request param "chunked"
	chunked := r.FormValue("chunked") == "true"
	chunkSize := DefaultChunkSize
	if chunked {
		if n, err := strconv.ParseInt(r.FormValue("chunk_size"), 10, 64); err == nil && int(n) > 0 {
			chunkSize = int(n)
		}
	}

	opts := query.ExecutionOptions{
		Database:   db,
		TimeToLive: ttl,
		Epoch:      epoch,
		ChunkSize:  chunkSize,
		ReadOnly:   r.Method == "GET",
	}

	closeCh := make(chan struct{})
	defer close(closeCh)
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

		// Write out result immediately if chunked.
		if chunked {
			_, _ = rw.WriteResponse(Response{Results: []*query.Result{ret}})
			w.(http.Flusher).Flush()
			continue
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

	// If it's not chunked we buffered everything in memory, so write it out
	if !chunked {
		_, _ = rw.WriteResponse(resp)
	}
}
