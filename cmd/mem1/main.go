package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nacfson/nacfson-memory/internal/config"
	"github.com/nacfson/nacfson-memory/internal/storage"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		printUsage(stderr)
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	switch args[0] {
	case "migrate":
		return runMigrate(ctx, args[1:], stdout, stderr)
	case "-h", "--help", "help":
		printUsage(stdout)
		return 0
	default:
		fmt.Fprintf(stderr, "unknown subcommand %q\n", args[0])
		printUsage(stderr)
		return 2
	}
}

func runMigrate(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("migrate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	down := fs.Bool("down", false, "roll back schema and River migrations")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	cfg, err := config.Load(config.ModeMigrate)
	if err != nil {
		fmt.Fprintf(stderr, "config error: %v\n", err)
		return 1
	}

	migCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	if err := storage.Migrate(migCtx, cfg.DatabaseURL, *down); err != nil {
		fmt.Fprintf(stderr, "migration failed: %v\n", err)
		return 1
	}
	if *down {
		fmt.Fprintln(stdout, "migrations rolled back")
	} else {
		fmt.Fprintln(stdout, "migrations applied")
	}
	return 0
}

func printUsage(w io.Writer) {
	fmt.Fprintln(w, "usage: mem1 <subcommand> [flags]")
	fmt.Fprintln(w, "subcommands:")
	fmt.Fprintln(w, "  migrate [--down]    apply or roll back PostgreSQL schema migrations")
}
