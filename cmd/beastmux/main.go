// Command beastmux multiplexes N upstream Mode S BEAST TCP streams
// into a single consolidated, deduplicated BEAST stream.
//
// Usage:
//
//	beastmux \
//	    --source receiver-a:30005 \
//	    --source receiver-b:30005 \
//	    --source receiver-c:30005 \
//	    --listen :30005 \
//	    --dedup-window 500ms \
//	    --reconnect-min 1s \
//	    --reconnect-max 30s \
//	    [--stdout-hex] \
//	    [--log-format text|json]
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/hyperized/demod1090/beastsrv"
)

const (
	logFormatText = "text"
	logFormatJSON = "json"

	defaultDedupWindow   = 500 * time.Millisecond
	defaultReconnectMin  = 1 * time.Second
	defaultReconnectMax  = 30 * time.Second
	defaultStatsInterval = 10 * time.Second
)

// version is overridden at link time with
// `-ldflags "-X main.version=$(git describe --tags --always --dirty)"`.
// The Makefile's build target already wires this in.
//
//nolint:gochecknoglobals // ldflags injection point — must be a package-level var.
var version = "0.0.0-dev"

// stringSliceFlag is a flag.Value that accumulates repeated string
// flags (--source can be passed multiple times).
type stringSliceFlag []string

func (s *stringSliceFlag) String() string { return strings.Join(*s, ",") }

func (s *stringSliceFlag) Set(value string) error {
	if value == "" {
		return errEmptyFlagValue
	}

	*s = append(*s, value)

	return nil
}

// errEmptyFlagValue is the sentinel returned when a repeatable flag
// is invoked with an empty argument (e.g. `--source ""`).
var errEmptyFlagValue = InvalidFlagError("empty value")

// InvalidFlagError captures a CLI validation failure. main exits 2
// on any InvalidFlagError; the message is already printed to stderr
// by parseFlags before the error is returned.
type InvalidFlagError string

func (e InvalidFlagError) Error() string { return "beastmux: " + string(e) }

func main() {
	cfg, err := parseFlags(os.Args[1:], os.Stderr)
	if err != nil {
		// flag.Parse already prints usage to stderr on parse errors.
		os.Exit(2)
	}

	if cfg.showVersion {
		_, _ = fmt.Fprintln(os.Stdout, version)

		return
	}

	logger := newLogger(cfg.logFormat, os.Stderr)

	if cfg.listenAddr != "" {
		cfg.server = beastsrv.NewServer(
			beastsrv.WithListenAddr(cfg.listenAddr),
			beastsrv.WithLogger(logger),
		)
	}

	if cfg.stdoutHex {
		cfg.hexOut = os.Stdout
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	err = run(ctx, cfg, logger)

	stop()

	if err != nil {
		logger.LogAttrs(ctx, slog.LevelError, "beastmux: fatal", slog.Any("err", err))
		os.Exit(1)
	}
}

// parseFlags consumes args (typically os.Args[1:]) and returns the
// resulting config. Returns a non-nil error when flag parsing fails
// or when required flags are missing. Output (usage text, errors) is
// written to stderr.
func parseFlags(args []string, stderr io.Writer) (config, error) {
	var (
		sources stringSliceFlag
		cfg     = defaultConfig()
	)

	flagSet := flag.NewFlagSet("beastmux", flag.ContinueOnError)
	flagSet.SetOutput(stderr)

	flagSet.Var(&sources, "source", "upstream BEAST endpoint (host:port); repeatable, at least one required")
	flagSet.StringVar(&cfg.listenAddr, "listen", cfg.listenAddr,
		`TCP listen address for the consolidated BEAST stream; empty disables the server`)
	flagSet.DurationVar(&cfg.dedupWindow, "dedup-window", cfg.dedupWindow,
		"duration a frame key suppresses duplicates for")
	flagSet.DurationVar(&cfg.reconnectMin, "reconnect-min", cfg.reconnectMin,
		"initial reconnect backoff after a source disconnects or fails to dial")
	flagSet.DurationVar(&cfg.reconnectMax, "reconnect-max", cfg.reconnectMax,
		"upper bound on reconnect backoff")
	flagSet.DurationVar(&cfg.statsInterval, "stats-interval", cfg.statsInterval,
		"interval between stats log lines; 0 disables periodic stats")
	flagSet.BoolVar(&cfg.stdoutHex, "stdout-hex", cfg.stdoutHex,
		"also emit each forwarded frame as a hex line on stdout")
	flagSet.StringVar(&cfg.logFormat, "log-format", cfg.logFormat,
		`log handler ("text" or "json")`)
	flagSet.BoolVar(&cfg.showVersion, "version", false, "print version and exit")

	if err := flagSet.Parse(args); err != nil {
		return config{}, fmt.Errorf("beastmux: parse flags: %w", err)
	}

	cfg.sources = []string(sources)

	if cfg.showVersion {
		// --version short-circuits all other validation; main prints
		// the version and exits before run() is called.
		return cfg, nil
	}

	if len(cfg.sources) == 0 {
		_, _ = fmt.Fprintln(stderr, "beastmux: --source is required (repeatable)")

		flagSet.Usage()

		return config{}, InvalidFlagError("--source is required")
	}

	if cfg.logFormat != logFormatText && cfg.logFormat != logFormatJSON {
		_, _ = fmt.Fprintf(stderr,
			"beastmux: --log-format must be %q or %q, got %q\n",
			logFormatText, logFormatJSON, cfg.logFormat)

		return config{}, InvalidFlagError("--log-format must be text or json")
	}

	return cfg, nil
}

// newLogger returns a slog.Logger with the configured handler. Falls
// back to the text handler for any unrecognised format — parseFlags
// validates the value, so this is defensive only.
func newLogger(format string, dst io.Writer) *slog.Logger {
	if format == logFormatJSON {
		return slog.New(slog.NewJSONHandler(dst, nil))
	}

	return slog.New(slog.NewTextHandler(dst, nil))
}

// defaultConfig returns the config populated with the documented
// CLI defaults.
func defaultConfig() config {
	return config{
		listenAddr:    beastsrv.DefaultListenAddr,
		dedupWindow:   defaultDedupWindow,
		reconnectMin:  defaultReconnectMin,
		reconnectMax:  defaultReconnectMax,
		statsInterval: defaultStatsInterval,
		logFormat:     logFormatText,
	}
}
