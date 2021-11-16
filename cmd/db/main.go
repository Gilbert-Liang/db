package main

import (
	"db/cmd/db/run"
	"db/cmd/db/serve"
	"fmt"
)

func main() {
	cmd := run.GetCommand()
	cmd.AddCommand(serve.GetCommand())
	if err := cmd.Execute(); err != nil {
		fmt.Printf("ERR: %s\n", err)
	}
}
