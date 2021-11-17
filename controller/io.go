package controller

import (
	"db/constant"
	"db/query"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
)

// Response represents a list of statement results.
type Response struct {
	Results []*query.Result
	Err     error
}

// MarshalJSON encodes a Response struct into JSON.
func (r Response) MarshalJSON() ([]byte, error) {
	// Define a struct that outputs "error" as a string.
	var o struct {
		Results []*query.Result `json:"results,omitempty"`
		Err     string          `json:"error,omitempty"`
	}

	// Copy fields to output struct.
	o.Results = r.Results
	if r.Err != nil {
		o.Err = r.Err.Error()
	}

	return json.Marshal(&o)
}

// UnmarshalJSON decodes the data into the Response struct.
func (r *Response) UnmarshalJSON(b []byte) error {
	var o struct {
		Results []*query.Result `json:"results,omitempty"`
		Err     string          `json:"error,omitempty"`
	}

	err := json.Unmarshal(b, &o)
	if err != nil {
		return err
	}
	r.Results = o.Results
	if o.Err != "" {
		r.Err = errors.New(o.Err)
	}
	return nil
}

// Error returns the first error from any statement.
// Returns nil if no errors occurred on any statements.
func (r *Response) Error() error {
	if r.Err != nil {
		return r.Err
	}
	for _, rr := range r.Results {
		if rr != nil {
			return rr.Err
		}
	}
	return nil
}

// ResponseWriter is an interface for writing a response.
type ResponseWriter interface {
	// WriteResponse writes a response.
	WriteResponse(resp Response) (int, error)

	http.ResponseWriter
	http.Flusher
}

func NewResponseWriter(w http.ResponseWriter, r *http.Request) ResponseWriter {
	pretty := r.URL.Query().Get("pretty") == "true"
	rw := &responseWriter{
		Pretty:         pretty,
		ResponseWriter: w,
	}

	w.Header().Add("Content-Type", "application/json")
	return rw
}

// responseWriter is an implementation of ResponseWriter.
type responseWriter struct {
	Pretty bool

	http.ResponseWriter
}

type bytesCountWriter struct {
	w io.Writer
	n int
}

func (w *bytesCountWriter) Write(data []byte) (int, error) {
	n, err := w.w.Write(data)
	w.n += n
	return n, err
}

func (w *responseWriter) WriteResponse(resp Response) (int, error) {
	writer := bytesCountWriter{w: w.ResponseWriter}
	err := w.writeJson(&writer, resp)
	return writer.n, err
}

func (w *responseWriter) writeJson(writer io.Writer, resp Response) (err error) {
	var b []byte
	if w.Pretty {
		b, err = json.MarshalIndent(resp, "", "    ")
	} else {
		b, err = json.Marshal(resp)
	}

	if err != nil {
		_, err = io.WriteString(writer, err.Error())
	} else {
		_, err = writer.Write(b)
	}

	_, _ = writer.Write([]byte("\n"))
	return err
}

func (w *responseWriter) Flush() {
	if fl, ok := w.ResponseWriter.(http.Flusher); ok {
		fl.Flush()
	}
}

func writeHeader(w http.ResponseWriter, code int) {
	w.WriteHeader(code)
}

func writeError(w http.ResponseWriter, errMsg string) {
	writeErrorWithCode(w, errMsg, http.StatusBadRequest)
}

func writeErrorWithCode(w http.ResponseWriter, errMsg string, code int) {
	if code/100 != 2 {
		sz := math.Min(float64(len(errMsg)), 1024.0)
		w.Header().Set(constant.HeaderErrorMsg, errMsg[:int(sz)])
	}

	w.Header().Add(constant.HeaderContentType, constant.ContentTypeJSON)
	writeHeader(w, code)

	response := &Response{Err: errors.New(errMsg)}
	b, _ := json.Marshal(response)
	_, _ = w.Write(b)
}
