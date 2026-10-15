package main

import (
	"context"
	"fmt"
	"os"

	cli "github.com/urfave/cli/v3"
)

func main() {
	if err := newCommand().Run(context.Background(), os.Args); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)

		os.Exit(1)
	}
}

func newCommand() *cli.Command {
	return &cli.Command{
		Name:     "operion",
		Usage:    "Workflow automation engine",
		Commands: []*cli.Command{newRunCommand()},
	}
}
