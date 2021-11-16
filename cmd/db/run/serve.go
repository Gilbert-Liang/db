package run

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"db/cmd/db/internal"

	"github.com/gin-gonic/gin"
)

func (a *Application) serverStart(cancel context.CancelFunc) {
	s := internal.NewServer()
	s.DebugMode = false
	s.EnableLogger = false
	s.Printer = os.Stderr
	a.server = s

	gin.SetMode(gin.ReleaseMode)
	if err := s.Init(); err != nil {
		fmt.Printf("ERR: initializing server: %s\n", err)
		return
	}

	go func() {
		if err := s.Run(); err != http.ErrServerClosed {
			fmt.Printf("ERR: starting server: %s\n", err)
		}
	}()
	cancel()

	<-a.quit
	go s.Close()

	select {
	case <-s.ClosedCh:
		return
	case <-time.After(time.Second * 30):
	}
}

func (a *Application) serverStop() {
	a.server.Close()
}
