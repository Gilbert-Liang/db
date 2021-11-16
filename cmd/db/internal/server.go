package internal

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	"db/config"
	"db/coordinator"
	"db/logger"
	"db/meta"
	"db/query"
	"db/server"
	"db/tsdb"

	"github.com/felixge/fgprof"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type Server struct {
	Addr         string
	DebugMode    bool
	EnableLogger bool

	server  http.Server
	gin     *gin.Engine
	Logger  *zap.Logger
	Printer io.Writer

	metaClient    *meta.Client
	tsdbStore     *tsdb.Store
	queryExecutor *query.Executor
	pointsWriter  *coordinator.PointsWriter

	ClosedCh chan struct{}
}

func NewServer() *Server {
	return &Server{
		Addr:         "localhost:8080",
		Printer:      os.Stdout,
		DebugMode:    true,
		EnableLogger: true,
		Logger:       zap.NewNop(),
		ClosedCh:     make(chan struct{}, 1),
	}
}

func (s *Server) Init() error {
	if s.EnableLogger {
		s.Logger = logger.New(os.Stderr)
	}

	s.gin = gin.New()
	if s.DebugMode {
		s.gin.Use(AccessLogMiddleware())
	}
	s.gin.Use(CorsMiddleware())

	if s.DebugMode {
		s.gin.GET("/debug/pprof", gin.WrapH(fgprof.Handler()))
	}

	if err := s.initDependencies(); err != nil {
		return err
	}
	if err := s.runDependencies(); err != nil {
		return err
	}

	rg := s.gin.Group("/cnosdb")
	s.registerPingAPI(rg)
	s.registerQueryAPI(rg)
	s.registerWriteAPI(rg)

	return nil
}

func getHomeDir() (string, error) {
	u, err := user.Current()
	if err == nil {
		return u.HomeDir, nil
	} else if os.Getenv("HOME") != "" {
		return os.Getenv("HOME"), nil
	} else {
		return "", fmt.Errorf("failed to determine current user for storage")
	}
}

func (s *Server) initDependencies() error {
	homeDir, err := getHomeDir()
	if err != nil {
		return fmt.Errorf("failed to determine current user for storage")
	}

	metaConf := meta.NewConfig()
	metaConf.Dir = filepath.Join(homeDir, ".base-query/meta")

	if err := os.MkdirAll(metaConf.Dir, 0777); err != nil {
		return fmt.Errorf("mkdir all: %s", err)
	}

	metaClient := meta.NewClient(metaConf)
	metaClient.WithLogger(s.Logger)
	s.metaClient = metaClient

	tsdbConf := tsdb.NewConfig()
	tsdbConf.Dir = filepath.Join(homeDir, ".base-query/data")
	tsdbConf.WALDir = filepath.Join(homeDir, ".base-query/wal")

	tsdbStore := tsdb.NewStore(tsdbConf.Dir)
	tsdbStore.EngineOptions.Config = tsdbConf
	tsdbStore.WithLogger(s.Logger)
	s.tsdbStore = tsdbStore

	pointsWriter := coordinator.NewPointsWriter()
	pointsWriter.MetaClient = metaClient
	pointsWriter.TSDBStore = tsdbStore
	pointsWriter.WithLogger(s.Logger)
	s.pointsWriter = pointsWriter

	queryExecutor := query.NewExecutor()
	queryExecutor.StatementExecutor = &coordinator.StatementExecutor{
		MetaClient:  metaClient,
		TaskManager: queryExecutor.TaskManager,
		TSDBStore:   tsdbStore,
		ShardMapper: &coordinator.LocalShardMapper{
			MetaClient: metaClient,
			TSDBStore:  tsdbStore,
		},
		PointsWriter: pointsWriter,
	}
	queryExecutor.WithLogger(s.Logger)
	s.queryExecutor = queryExecutor

	return nil
}

func (s *Server) runDependencies() error {
	if err := s.metaClient.Open(); err != nil {
		return fmt.Errorf("open meta client: %s", err)
	}

	// Open TSDB store.
	if err := s.tsdbStore.Open(); err != nil {
		return fmt.Errorf("open tsdb store: %s", err)
	}

	// Open the points writer service
	if err := s.pointsWriter.Open(); err != nil {
		return fmt.Errorf("open points writer: %s", err)
	}

	return nil
}

func (s *Server) registerPingAPI(g *gin.RouterGroup) {
	g.GET("/ping", gin.WrapF(server.Ping))
}

func (s *Server) registerQueryAPI(g *gin.RouterGroup) {
	c := &config.HTTP{}
	api := &server.QueryAPI{
		Config:        c,
		QueryExecutor: s.queryExecutor,
	}
	ginApi := gin.WrapH(api)

	g.GET("/query", ginApi)
	g.POST("/query", ginApi)
}

func (s *Server) registerWriteAPI(g *gin.RouterGroup) {
	c := &config.HTTP{}
	api := &server.WriteAPI{
		Config:       c,
		MetaClient:   s.metaClient,
		PointsWriter: s.pointsWriter,
	}

	g.POST("/write", gin.WrapH(api))
}

func (s *Server) Run() error {
	s.server.Handler = s.gin

	ln, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}
	return s.server.Serve(ln)
}

func (s *Server) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	_ = s.server.Shutdown(ctx)
	defer cancel()

	if s.pointsWriter != nil {
		_ = s.pointsWriter.Close()
	}

	if s.queryExecutor != nil {
		_ = s.queryExecutor.Close()
	}

	if s.tsdbStore != nil {
		_ = s.tsdbStore.Close()
	}

	if s.metaClient != nil {
		_ = s.metaClient.Close()
	}

	close(s.ClosedCh)
}

func AccessLogMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		r := c.Request
		defer func() {
			path := r.RequestURI
			unescapedPath, err := url.PathUnescape(path)
			if err != nil {
				unescapedPath = path
			}
			// LogFormat "%h %l %u %t \"%r\" %>s %b" common
			// 127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326
			ip, _ := c.RemoteIP()
			requestInfo := fmt.Sprintf("%s %dus \"%s %s %s\" %d %d", ip.String(), time.Since(start).Microseconds(),
				r.Method, unescapedPath, r.Proto, c.Writer.Status(), c.Writer.Size())

			r := recover()
			switch {
			case r != nil:
				b := make([]byte, 1024)
				written := runtime.Stack(b, false)
				if written == 1024 {
					fmt.Printf("ERR: %s\nStack:%s...\n", requestInfo, string(b))
				} else {
					fmt.Printf("ERR: %s\nStack:%s\n", requestInfo, string(b))
				}
			case len(c.Errors) > 0:
				fmt.Printf("ERR: %s : '%s'\n", requestInfo, c.Errors[0].Err)
			default:
				fmt.Printf("%s\n", requestInfo)
			}
		}()
		c.Next()
	}
}

func CorsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		origin := c.Request.Header.Get("Origin")
		if len(origin) == 0 {
			// request is not a CORS request
			return
		}
		host := c.Request.Host

		if origin == "http://"+host || origin == "https://"+host {
			// request is not a CORS request but have origin header.
			// for example, use fetch api
			return
		}

		if c.Request.Method == "OPTIONS" {
			header := c.Writer.Header()
			header.Set("Access-Control-Allow-Credentials", "true")
			header.Set("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE")
			header.Set("Access-Control-Allow-Headers", "Origin,Content-Length,Content-Type")
			header.Set("Access-Control-Max-Age", strconv.FormatInt(int64(12*time.Hour/time.Second), 10))
			header.Set("Access-Control-Allow-Origin", "*")
			defer c.AbortWithStatus(http.StatusNoContent) // Using 204 is better than 200 when the request status is OPTIONS
		} else {
			header := make(http.Header)
			header.Set("Access-Control-Allow-Credentials", "true")
			header.Set("Access-Control-Allow-Origin", "*")
		}
	}
}
