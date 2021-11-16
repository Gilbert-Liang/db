package run

import (
	"fmt"
	"os"

	"db/cmd/db/internal"
	"db/pkg/cmdutil"

	"github.com/spf13/cobra"
)

func GetCommand() *cobra.Command {
	c := &cobra.Command{
		Use:               "run",
		CompletionOptions: cmdutil.GetDefaultCompletionOptions(),
		Run: func(cmd *cobra.Command, args []string) {
			if err := app.run(); err != nil {
				_, _ = fmt.Fprintf(os.Stderr, "%s\n", err)
			}
		},
	}
	SetFlags(c)
	return c
}

func SetFlags(c *cobra.Command) {
	flags := c.Flags()
	flags.StringVar(&app.host, "host", internal.DEFAULT_HOST, "Host of the DB instance to connect to.")
	flags.IntVar(&app.port, "port", internal.DEFAULT_PORT, "Port of the DB instance to connect to.")
	flags.BoolVar(&app.needConnect, "need-connect", true, "Connect to DB instance first.")
	flags.StringVar(&app.format, "format", "column", "The format of the server responses:  json, csv, or column.")
	flags.BoolVar(&app.pretty, "pretty", false, "Turns on pretty print for the json format.")
	flags.StringVar(&app.precision, "precision", "ns", "The format of the timestamp:  rfc3339,h,m,s,ms,u or ns.")
}
