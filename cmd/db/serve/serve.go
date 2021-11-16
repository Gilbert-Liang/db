package serve

import (
	"db/cmd/db/internal"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "db/tsdb/engine"
	_ "db/tsdb/index"

	"github.com/spf13/cobra"
)

func GetCommand() *cobra.Command {
	c := &cobra.Command{
		Use: "serve",
		Run: func(cmd *cobra.Command, args []string) {
			s := internal.NewServer()

			if err := s.Init(); err != nil {
				fmt.Printf("ERR: initializing server: %s\n", err)
				return
			}

			signalCh := make(chan os.Signal, 1)
			signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)

			go func() {
				if err := s.Run(); err != http.ErrServerClosed {
					fmt.Printf("ERR: starting server: %s\n", err)
				}
			}()

			<-signalCh
			go s.Close()

			select {
			case <-signalCh:
				fmt.Println("Second signal received, initializing hard shutdown")
				return
			case <-s.ClosedCh:
				fmt.Println("Server closed")
				return
			case <-time.After(time.Second * 30):
				fmt.Println("Time limit reached, initializing hard shutdown")
			}
		},
	}

	return c
}
