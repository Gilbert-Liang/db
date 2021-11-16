package run

import (
	"bytes"
	"context"
	"db/cmd/db/internal"
	models "db/model"
	"db/parser/cnosql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/signal"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/peterh/liner"
)

const (
	CacheKey_AST = "AST"
)

var app = &Application{
	format:     "column",
	precision:  "ns",
	pretty:     false,
	connected:  false,
	variables:  make(map[string]string),
	osSignals:  make(chan os.Signal, 1),
	quit:       make(chan struct{}, 1),
	stopServer: make(chan struct{}, 1),
}

type Application struct {
	line   *liner.State
	client *internal.Client
	server *internal.Server

	host        string
	port        int
	needConnect bool
	database    string
	timeToLive  string
	format      string
	precision   string
	pretty      bool

	serverVersion string
	connected     bool
	variables     map[string]string

	osSignals  chan os.Signal
	quit       chan struct{}
	stopServer chan struct{}
}

func (a *Application) run() error {
	a.line = liner.NewLiner()
	defer func(line *liner.State) {
		if err := line.Close(); err != nil {
			fmt.Printf("ERR: cannot close the Liner: %s\n", err)
		}
	}(a.line)

	if a.needConnect {
		if err := a.connect(""); err != nil {
			return err
		}
	}

	signal.Notify(a.osSignals, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case <-a.osSignals:
			a.exit()
			return nil
		case <-a.quit:
			a.exit()
			return nil
		default:
			l, err := a.line.Prompt("> ")
			if err == io.EOF {
				l = "exit"
			} else if err != nil {
				a.exit()
				return err
			}
			a.line.AppendHistory(l)
			if err = a.parseCommand(l); err != nil {
				fmt.Printf("ERR: %s\n", err)
			}
		}
	}
}

func (a *Application) parseCommand(cmd string) error {
	tokens := strings.Fields(strings.TrimSpace(strings.ToLower(cmd)))
	if len(tokens) > 0 {
		switch tokens[0] {
		case "exit", "quit":
			close(a.quit)
		case "server":
			return a.embeddedServer(cmd)
		case "connect":
			return a.connect(cmd)
		case "use":
			a.setDbAndTtl(cmd)
		case "format":
			a.setFormat(cmd)
		case "precision":
			a.setPrecision(cmd)
		case "pretty":
			a.pretty = !a.pretty
			if a.pretty {
				fmt.Println("Pretty print enabled")
			} else {
				fmt.Println("Pretty print disabled")
			}
		case "history":
			a.printHistory()
		case "env":
			a.printEnv()
		case "load":
			return a.loadAst(cmd)
		case "insert":
			return a.requestInsert(cmd)
		default:
			return a.requestQuery(cmd)
		}
	}
	return nil
}

func (a *Application) embeddedServer(cmd string) error {
	args := strings.SplitAfterN(strings.TrimSuffix(strings.TrimSpace(cmd), ";"), " ", 2)
	if len(args) != 2 {
		return fmt.Errorf("ERR: could not parse database name from %q", cmd)
	}

	switch args[1] {
	case "start":
		fmt.Println("Embedded server starting.")
		ctx, cancel := context.WithCancel(context.Background())
		go a.serverStart(cancel)
		<-ctx.Done()
		fmt.Println("Embedded server started.")
		return a.connect("")
	case "stop":
		fmt.Println("Embedded server stopping.")
		a.serverStop()
		fmt.Println("Embedded server stopped, you may need to use \"connect\" command to connect to a DB instance.")
	}
	return nil
}

func (a *Application) connect(cmd string) error {
	cmd = strings.ToLower(cmd)

	var addr string
	if cmd == "" {
		// connect 命令参数为空时，使用当前配置
		addr = net.JoinHostPort(a.host, strconv.Itoa(a.port))
	} else {
		addr = strings.TrimSpace(strings.Replace(cmd, "connect", "", 1))
	}

	var _url url.URL
	{
		var host string
		var port int
		h, p, err := net.SplitHostPort(addr)
		if err != nil {
			if addr == "" {
				host = internal.DEFAULT_HOST
			} else {
				host = addr
			}
			port = internal.DEFAULT_PORT
		} else {
			host = h
			port, err = strconv.Atoi(p)
			if err != nil {
				return fmt.Errorf("invalid port number %q: %s\n", addr, err)
			}
		}
		_url = url.URL{
			Scheme: "http",
			Host:   host,
		}
		if port != 80 {
			_url.Host = net.JoinHostPort(host, strconv.Itoa(port))
		}
	}
	_url.Path = "/cnosdb"
	newAddr := _url.String()

	cli1, err := internal.NewHTTPClient(newAddr)
	if err != nil {
		return fmt.Errorf("could not create client: %s", err)
	}
	a.client = cli1

	_, v, err := a.client.Ping(10 * time.Second)
	if err != nil {
		return err
	}
	a.serverVersion = v

	// 更新配置信息
	if host, port, err := net.SplitHostPort(newAddr); err == nil {
		a.host = host
		if i, err := strconv.Atoi(port); err == nil {
			a.port = i
		}
	}

	return nil
}

// load 加载 ast 数据至 CacheKey_AST
func (a *Application) loadAst(cmd string) error {

	a.variables[CacheKey_AST] = ""
	return nil
}

// printAst 显示加载的 ast 数据
func (a *Application) printAst() {

	// TODO
	if s, ok := a.variables[CacheKey_AST]; ok {
		fmt.Println(s)
	}
}

func (a *Application) requestInsert(cmd string) error {
	bp, err := a.parseInsert(cmd)
	if err != nil {
		return nil
	}
	if err := a.client.Write(bp); err != nil {
		if a.database == "" {
			fmt.Println("Note: error may be due to not setting a database or ttl.")
			fmt.Println(`Please set a database with the command "use <database>" or`)
			fmt.Println("INSERT INTO <database>.<time-to-live> <point>")
		}
	}
	return nil
}

func (a *Application) parseInsert(stmt string) (*internal.BatchPoints, error) {
	ident, point := parseNextIdentifier(stmt)
	if !strings.EqualFold(ident, "insert") {
		return nil, fmt.Errorf("found %s, expected INSERT", ident)
	}

	ps, err := parsePoints(point)
	if err != nil {
		return nil, errors.Unwrap(err)
	}

	bp, err := internal.NewBatchPoints(a.precision, a.database, a.timeToLive)
	if err != nil {
		return nil, errors.Unwrap(err)
	}
	bp.Points = append(bp.Points, ps...)

	return bp, nil
}

func (a *Application) requestQuery(query string) error {
	if a.timeToLive != "" {
		pq, err := cnosql.NewParser(strings.NewReader(query)).ParseQuery()
		if err != nil {
			return err
		}
		for _, stmt := range pq.Statements {
			if selectStatement, ok := stmt.(*cnosql.SelectStatement); ok {
				cnosql.WalkFunc(selectStatement.Sources, func(n cnosql.Node) {
					if t, ok := n.(*cnosql.Metric); ok {
						if t.Database == "" && a.database != "" {
							t.Database = a.database
						}
						if t.TimeToLive == "" && a.timeToLive != "" {
							t.TimeToLive = a.timeToLive
						}
					}
				})
			}
		}
		query = pq.String()
	}

	req := &internal.Query{
		Command:    query,
		Database:   a.database,
		TimeToLive: a.timeToLive,
	}

	response, err := a.client.Query(req)
	if err != nil {
		return err
	}
	a.writeResponse(response, os.Stdout)
	if err := response.Error(); err != nil {
		if a.database == "" {
			fmt.Println("Warning: It is possible this error is due to not setting a database.")
			fmt.Println(`Please set a database with the command "use <database>".`)
		}
		return err
	}
	return nil
}

// writeResponse 输出结果集
func (c *Application) writeResponse(response *internal.Response, w io.Writer) {
	switch c.format {
	case "json":
		c.writeJSON(response, w)
	case "column":
		c.writeColumns(response, w)
	default:
		_, _ = fmt.Fprintf(w, "Unknown output format %q.\n", c.format)
	}
}

func (c *Application) writeJSON(response *internal.Response, w io.Writer) {
	var d []byte
	var err error
	if c.pretty {
		d, err = json.MarshalIndent(response, "", "    ")
	} else {
		d, err = json.Marshal(response)
	}
	if err != nil {
		_, _ = fmt.Fprintf(w, "ERR: unable to parse json: %s\n", err)
		return
	}
	_, _ = fmt.Fprintln(w, string(d))
}

func tagsEqual(prev, current map[string]string) bool {
	return reflect.DeepEqual(prev, current)
}

func columnsEqual(prev, current []string) bool {
	return reflect.DeepEqual(prev, current)
}

func headersEqual(prev, current models.Row) bool {
	if prev.Name != current.Name {
		return false
	}
	return tagsEqual(prev.Tags, current.Tags) && columnsEqual(prev.Columns, current.Columns)
}

func interfaceToString(v interface{}) string {
	switch t := v.(type) {
	case nil:
		return ""
	case bool:
		return fmt.Sprintf("%v", v)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, uintptr:
		return fmt.Sprintf("%d", t)
	case float32, float64:
		return fmt.Sprintf("%v", t)
	default:
		return fmt.Sprintf("%v", t)
	}
}

// formatResultSeries 格式化结果集，对于 csv 和 column 格式有不同的输出结果
func (c *Application) formatResultSeries(result internal.Result, separator string, suppressHeaders bool) []string {
	var rows []string
	for i, row := range result.Series {
		var tags []string
		for k, v := range row.Tags {
			tags = append(tags, fmt.Sprintf("%s=%s", k, v))
		}
		sort.Strings(tags)

		var columnNames []string

		// 对于 csv 格式，将 name 和 tag 作为列输出
		if c.format == "csv" {
			if len(tags) > 0 {
				columnNames = append([]string{"tags"}, columnNames...)
			}

			if row.Name != "" {
				columnNames = append([]string{"name"}, columnNames...)
			}
		}

		columnNames = append(columnNames, row.Columns...)

		// 对于 column 格式，若结果集中存在多个序列条目，不同的序列条目间输出空行
		if i > 0 && c.format == "column" && !suppressHeaders {
			rows = append(rows, "")
		}

		// 对于 column 格式，将 name 和 tag 各自输出到单独的行
		if c.format == "column" && !suppressHeaders {
			if row.Name != "" {
				n := fmt.Sprintf("name: %s", row.Name)
				rows = append(rows, n)
			}
			if len(tags) > 0 {
				t := fmt.Sprintf("tags: %s", (strings.Join(tags, ", ")))
				rows = append(rows, t)
			}
		}

		if !suppressHeaders {
			rows = append(rows, strings.Join(columnNames, separator))
		}

		// 对于 column 模式，在列名下输出 "-"
		if c.format == "column" && !suppressHeaders {
			lines := make([]string, len(columnNames))
			for colIdx, colName := range columnNames {
				lines[colIdx] = strings.Repeat("-", len(colName))
			}
			rows = append(rows, strings.Join(lines, separator))
		}

		for _, v := range row.Values {
			var values []string
			if c.format == "csv" {
				if row.Name != "" {
					values = append(values, row.Name)
				}
				if len(tags) > 0 {
					values = append(values, strings.Join(tags, ","))
				}
			}

			for _, vv := range v {
				values = append(values, interfaceToString(vv))
			}
			rows = append(rows, strings.Join(values, separator))
		}
	}
	return rows
}

func (c *Application) writeCSV(response *internal.Response, w io.Writer) {
	cw := csv.NewWriter(w)
	var previousHeaders models.Row
	for _, result := range response.Results {
		suppressHeaders := len(result.Series) > 0 && headersEqual(previousHeaders, result.Series[0])
		if !suppressHeaders && len(result.Series) > 0 {
			previousHeaders = models.Row{
				Name:    result.Series[0].Name,
				Tags:    result.Series[0].Tags,
				Columns: result.Series[0].Columns,
			}
		}

		rows := c.formatResultSeries(result, "\t", suppressHeaders)
		for _, r := range rows {
			_ = cw.Write(strings.Split(r, "\t"))
		}
	}
	cw.Flush()
}

func (c *Application) writeColumns(response *internal.Response, w io.Writer) {
	writer := new(tabwriter.Writer)
	writer.Init(w, 0, 8, 1, ' ', 0)

	var previousHeaders models.Row
	for i, result := range response.Results {
		// 1. 输出 Messages
		for _, m := range result.Messages {
			_, _ = fmt.Fprintf(w, "%s: %s.\n", m.Level, m.Text)
		}
		// Check to see if the headers are the same as the previous row.  If so, suppress them in the output
		suppressHeaders := len(result.Series) > 0 && headersEqual(previousHeaders, result.Series[0])
		if !suppressHeaders && len(result.Series) > 0 {
			previousHeaders = models.Row{
				Name:    result.Series[0].Name,
				Tags:    result.Series[0].Tags,
				Columns: result.Series[0].Columns,
			}
		}

		// If we are suppressing headers, don't output the extra line return. If we
		// aren't suppressing headers, then we put out line returns between results
		// (not before the first result, and not after the last result).
		if !suppressHeaders && i > 0 {
			_, _ = fmt.Fprintln(writer, "")
		}

		rows := c.formatResultSeries(result, "\t", suppressHeaders)
		for _, r := range rows {
			_, _ = fmt.Fprintln(writer, r)
		}

	}
	_ = writer.Flush()
}

func (a *Application) setDbAndTtl(cmd string) {
	args := strings.SplitAfterN(strings.TrimSuffix(strings.TrimSpace(cmd), ";"), " ", 2)
	if len(args) != 2 {
		fmt.Printf("ERR: could not parse database name from %q.\n", cmd)
		return
	}

	stmt := args[1]
	db, ttl, err := parseDatabaseAndTimeToLive([]byte(stmt))
	if err != nil {
		fmt.Printf("ERR: unable to parse database or ttl from %s\n", stmt)
		return
	}

	if !a.requestDbExists(db) {
		fmt.Printf("ERR: database %s does not exist!\n", db)
		return
	}

	a.database = db
	fmt.Printf("Using database %s\n", db)

	if ttl != "" {
		if !a.requestTtlExists(db, ttl) {
			return
		}
		a.timeToLive = ttl
		fmt.Printf("Using ttl %s\n", ttl)
	}
}

func parseDatabaseAndTimeToLive(stmt []byte) (string, string, error) {
	var db, ttl []byte
	var quoted bool
	var separatorCount int

	stmt = bytes.TrimSpace(stmt)

	for _, b := range stmt {
		if b == '"' {
			quoted = !quoted
			continue
		}
		if b == '.' && !quoted {
			separatorCount++
			if separatorCount > 1 {
				return "", "", errors.New(fmt.Sprintf("unable to parse database and ttl from %s", string(stmt)))
			}
			continue
		}
		if separatorCount == 1 {
			ttl = append(ttl, b)
			continue
		}
		db = append(db, b)
	}
	return string(db), string(ttl), nil
}

// requestDbExists Server交互：检查数据库是否存在
func (a *Application) requestDbExists(db string) bool {
	response, err := a.client.Query(&internal.Query{Command: "SHOW DATABASES"})
	if err != nil {
		fmt.Printf("ERR: %s\n", err)
		return false
	} else if err := response.Error(); err != nil {
		fmt.Printf("WARN: %s\n", err)
	} else {
		if databaseExists := func() bool {
			for _, result := range response.Results {
				for _, row := range result.Series {
					if row.Name == "databases" {
						for _, values := range row.Values {
							for _, database := range values {
								if database == db {
									return true
								}
							}
						}
					}
				}
			}
			return false
		}(); !databaseExists {
			fmt.Printf("ERR: database %s doesn't exist. Run SHOW DATABASES for a list of existing databases.\n", db)
			return false
		}
	}
	return true
}

// requestTtlExists Server 交互：检查保留策略是否存在
func (c *Application) requestTtlExists(db, ttl string) bool {
	response, err := c.client.Query(&internal.Query{Command: fmt.Sprintf("SHOW TTLS ON %q", db)})
	if err != nil {
		fmt.Printf("ERR: %s\n", err)
		return false
	} else if err := response.Error(); err != nil {
		fmt.Printf("WARN: %s\n", err)
	} else {
		if timeToLiveExists := func() bool {
			for _, result := range response.Results {
				for _, row := range result.Series {
					for _, values := range row.Values {
						for i, v := range values {
							if i != 0 {
								continue
							}
							if v == ttl {
								return true
							}
						}
					}
				}
			}
			return false
		}(); !timeToLiveExists {
			fmt.Printf("ERR: TTL %s doesn't exist. Run SHOW TTLS ON %q for a list of existing ttls.\n", ttl, db)
			return false
		}
	}
	return true
}

func (a *Application) setFormat(cmd string) {
	cmd = strings.ToLower(cmd)
	// Remove the "format" keyword if it exists
	cmd = strings.TrimSpace(strings.Replace(cmd, "format", "", -1))

	switch cmd {
	case "json", "csv", "column":
		a.format = cmd
	default:
		fmt.Printf("ERR: unknown format %q. Please use json, csv, or column.\n", cmd)
	}
}

func (a *Application) setPrecision(cmd string) {
	// normalize cmd
	cmd = strings.ToLower(cmd)

	// Remove the "precision" keyword if it exists
	cmd = strings.TrimSpace(strings.Replace(cmd, "precision", "", -1))

	switch cmd {
	case "h", "m", "s", "ms", "u", "ns":
		a.precision = cmd
	case "rfc3339":
		a.precision = ""
	default:
		fmt.Printf("ERR: unknown precision %q. Please use rfc3339, h, m, s, ms, u or ns.\n", cmd)
	}
}

func (a *Application) printEnv() {
	w := new(tabwriter.Writer)
	w.Init(os.Stdout, 0, 1, 1, ' ', 0)
	_, _ = fmt.Fprintln(w, "Setting\tValue")
	_, _ = fmt.Fprintln(w, "--------\t--------")
	_, _ = fmt.Fprintf(w, "Host\t%s\n", a.host)
	_, _ = fmt.Fprintf(w, "Port\t%s\n", a.port)
	_, _ = fmt.Fprintf(w, "Database\t%s\n", a.database)
	_, _ = fmt.Fprintf(w, "TimeToLive\t%s\n", a.timeToLive)
	_, _ = fmt.Fprintf(w, "Format\t%s\n", a.format)
	_, _ = fmt.Fprintf(w, "Precision\t%s\n", a.precision)
	_ = w.Flush()
}

func (a *Application) printHistory() {
	var buf bytes.Buffer
	if _, err := a.line.WriteHistory(&buf); err != nil {
		fmt.Printf("ERR: cannot close the Liner: %s\n", err)
	} else {
		fmt.Print(buf.String())
	}
}

func (a *Application) exit() {
	_ = a.line.Close()
	a.line = nil
}
