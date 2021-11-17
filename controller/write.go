package controller

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	cnosdb "db"
	"db/config"
	"db/coordinator"
	"db/meta"
	"db/model"
	"db/tsdb"

	"go.uber.org/zap"
)

var (
	errTruncated = errors.New("Read: truncated")
)

type WriteAPI struct {
	Config *config.HTTP

	Logger *zap.Logger

	MetaClient   *meta.Client
	PointsWriter *coordinator.PointsWriter
}

func (h *WriteAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	precision := r.URL.Query().Get("precision")
	switch precision {
	case "", "n", "ns", "u", "ms", "s", "m", "h":
		// it's valid
	default:
		writeError(w, fmt.Sprintf("invalid precision %q (use n, u, ms, s, m or h)", precision))
	}

	database := r.URL.Query().Get("db")
	timeToLive := r.URL.Query().Get("ttl")

	if database == "" {
		writeError(w, "database is required")
		return
	}

	if di := h.MetaClient.Database(database); di == nil {
		writeErrorWithCode(w, fmt.Sprintf("database not found: %q", database), http.StatusNotFound)
		return
	}

	body := r.Body
	if h.Config.MaxBodySize > 0 {
		body = truncateReader(body, int64(h.Config.MaxBodySize))
	}

	// Handle gzip decoding of the body
	if r.Header.Get("Content-Encoding") == "gzip" {
		b, err := gzip.NewReader(r.Body)
		if err != nil {
			writeError(w, err.Error())
			return
		}
		defer b.Close()
		body = b
	}

	var bs []byte
	if r.ContentLength > 0 {
		if h.Config.MaxBodySize > 0 && r.ContentLength > int64(h.Config.MaxBodySize) {
			writeErrorWithCode(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
			return
		}

		// This will just be an initial hint for the gzip reader, as the
		// bytes.Buffer will grow as needed when ReadFrom is called
		bs = make([]byte, 0, r.ContentLength)
	}
	buf := bytes.NewBuffer(bs)

	_, err := buf.ReadFrom(body)
	if err != nil {
		if err == errTruncated {
			writeErrorWithCode(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
			return
		}

		if h.Config.WriteTracing {
			h.Logger.Info("Write Handler unable to read bytes from request body")
		}
		writeError(w, err.Error())
		return
	}

	if h.Config.WriteTracing {
		h.Logger.Info("Write body received by Handler", zap.ByteString("body", buf.Bytes()))
	}

	points, parseError := model.ParsePointsWithPrecision(buf.Bytes(), time.Now().UTC(), precision)
	// Not points parsed correctly so return the error now
	if parseError != nil && len(points) == 0 {
		if parseError.Error() == "EOF" {
			writeHeader(w, http.StatusOK)
			return
		}
		writeError(w, parseError.Error())
		return
	}

	// Write points.
	if err := h.PointsWriter.WritePoints(database, timeToLive, points); cnosdb.IsClientError(err) {
		writeError(w, err.Error())
		return
	} else if werr, ok := err.(tsdb.PartialWriteError); ok {
		writeError(w, werr.Error())
		return
	} else if err != nil {
		writeErrorWithCode(w, err.Error(), http.StatusInternalServerError)
		return
	} else if parseError != nil {
		// We wrote some of the points
		// The other points failed to parse which means the client sent invalid line protocol.  We return a 400
		// response code as well as the lines that failed to parse.
		writeError(w, tsdb.PartialWriteError{Reason: parseError.Error()}.Error())
		return
	}

	writeHeader(w, http.StatusNoContent)
}

// A truncatedReader limits the amount of data returned to a maximum of r.N bytes.
type truncatedReader struct {
	r *io.LimitedReader
	io.Closer
}

func (r *truncatedReader) Read(p []byte) (n int, err error) {
	n, err = r.r.Read(p)
	if r.r.N <= 0 {
		return n, errTruncated
	}

	return n, err
}

func (r *truncatedReader) Close() error {
	if r.Closer != nil {
		return r.Closer.Close()
	}
	return nil
}

// truncateReader returns a Reader that reads from r
// but stops with ErrTruncated after n bytes.
func truncateReader(r io.Reader, n int64) io.ReadCloser {
	tr := &truncatedReader{r: &io.LimitedReader{R: r, N: n + 1}}

	if rc, ok := r.(io.Closer); ok {
		tr.Closer = rc
	}

	return tr
}
