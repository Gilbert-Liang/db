package internal

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"time"
)

const (
	DEFAULT_HOST     = "localhost"
	DEFAULT_PORT     = 8080
	DEFAULT_TIMEOUT  = 0
	DefaultUserAgent = "CnosDBClient"
)

type Query struct {
	Command    string
	Database   string
	TimeToLive string
	Precision  string
	Chunked    bool
	ChunkSize  int
	Parameters map[string]interface{}
}

type Client struct {
	url        url.URL
	useragent  string
	httpClient *http.Client
}

func NewHTTPClient(addr string) (*Client, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return nil, err
	} else if u.Scheme != "http" && u.Scheme != "https" {
		m := fmt.Sprintf("Unsupported protocol scheme: %s, your address"+
			" must start with http:// or https://", u.Scheme)
		return nil, errors.New(m)
	}

	return &Client{
		url:        *u,
		useragent:  DefaultUserAgent,
		httpClient: &http.Client{},
	}, nil
}

func (c *Client) Ping(timeout time.Duration) (time.Duration, string, error) {
	now := time.Now()

	u := c.url
	u.Path = path.Join(u.Path, "ping")

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return 0, "", err
	}

	req.Header.Set("User-Agent", c.useragent)

	if timeout > 0 {
		params := req.URL.Query()
		params.Set("wait_for_leader", fmt.Sprintf("%.0fs", timeout.Seconds()))
		req.URL.RawQuery = params.Encode()
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer call(resp.Body.Close)

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, "", err
	}

	if resp.StatusCode != http.StatusNoContent {
		var err = errors.New(string(body))
		return 0, "", err
	}

	version := resp.Header.Get("X-Cnosdb-Version")
	return time.Since(now), version, nil
}

func (c *Client) createQuery(q *Query) (*http.Request, error) {
	u := c.url
	u.Path = path.Join(u.Path, "query")

	jsonParameters, err := json.Marshal(q.Parameters)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", u.String(), nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "")
	req.Header.Set("User-Agent", c.useragent)

	params := req.URL.Query()
	params.Set("q", q.Command)
	params.Set("db", q.Database)
	if q.TimeToLive != "" {
		params.Set("ttl", q.TimeToLive)
	}
	params.Set("params", string(jsonParameters))

	if q.Precision != "" {
		params.Set("epoch", q.Precision)
	}
	req.URL.RawQuery = params.Encode()

	return req, nil

}

func (c *Client) Query(q *Query) (*Response, error) {
	req, err := c.createQuery(q)
	if err != nil {
		return nil, err
	}
	params := req.URL.Query()
	if q.Chunked {
		params.Set("chunked", "true")
		if q.ChunkSize > 0 {
			params.Set("chunk_size", strconv.Itoa(q.ChunkSize))
		}
		req.URL.RawQuery = params.Encode()
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer call(resp.Body.Close)

	if err := checkResponse(resp); err != nil {
		return nil, err
	}

	var response Response
	if q.Chunked {
		cr := NewChunkedResponse(resp.Body)
		for {
			r, err := cr.NextResponse()
			if err != nil {
				if err == io.EOF {
					break
				}
				// If we got an error while decoding the response, send that back.
				return nil, err
			}

			if r == nil {
				break
			}

			response.Results = append(response.Results, r.Results...)
			if r.Err != "" {
				response.Err = r.Err
				break
			}
		}
	} else {
		dec := json.NewDecoder(resp.Body)
		dec.UseNumber()
		decErr := dec.Decode(&response)

		// 若错误类型为 EOF ，忽视该错误
		if decErr != nil && decErr.Error() == "EOF" && resp.StatusCode != http.StatusOK {
			decErr = nil
		}
		// 对于其他错误类型，生成一个 error 并返回
		if decErr != nil {
			return nil, fmt.Errorf("unable to decode json: received status code %d err: %s", resp.StatusCode, decErr)
		}
	}

	// 若服务端未返回错误信息，但返回状态不是 HTTP Status OK ，则生成一个 error 并返回
	if resp.StatusCode != http.StatusOK && response.Error() == nil {
		return &response, fmt.Errorf("received status code %d from server", resp.StatusCode)
	}
	return &response, nil
}

func (c *Client) Write(bp *BatchPoints) error {
	var b bytes.Buffer

	for _, p := range bp.Points {
		if p == nil {
			continue
		}
		if _, err := b.WriteString(p.pt.PrecisionString(bp.Precision)); err != nil {
			return err
		}

		if err := b.WriteByte('\n'); err != nil {
			return err
		}
	}

	u := c.url
	u.Path = path.Join(u.Path, "write")

	req, err := http.NewRequest("POST", u.String(), &b)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "")
	req.Header.Set("User-Agent", c.useragent)

	params := req.URL.Query()
	params.Set("db", bp.Database)
	params.Set("ttl", bp.TimeToLive)
	params.Set("precision", bp.Precision)
	req.URL.RawQuery = params.Encode()

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer call(resp.Body.Close)

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		var err = errors.New(string(body))
		return err
	}

	return nil
}

type callFunc func() error

func call(fn callFunc) {
	if err := fn(); err != nil {
		printErr(err)
	}
}

func printErr(err error) {
	fmt.Printf("ERR: %s\n", err)
}

func checkResponse(resp *http.Response) error {
	// 若 Response 的格式不是 application/json
	if cType, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type")); cType != "application/json" {
		// 读取 1kb 数据，以帮助确认错误
		body, err := ioutil.ReadAll(io.LimitReader(resp.Body, 1024))
		if err != nil || len(body) == 0 {
			return fmt.Errorf("expected json response, got empty body, with status: %v", resp.StatusCode)
		}

		return fmt.Errorf("expected json response, got %q, with status: %v and response body: %q", cType, resp.StatusCode, body)
	}
	return nil
}
