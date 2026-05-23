// Package logging wires zerolog with a consistent service-tagged shape so
// log records from every binary land in the same schema for shipping.
package logging

import (
	"io"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

// New builds a logger configured for the named service.
func New(service, level string) zerolog.Logger {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	lvl, err := zerolog.ParseLevel(strings.ToLower(level))
	if err != nil || lvl == zerolog.NoLevel {
		lvl = zerolog.InfoLevel
	}

	var out io.Writer = os.Stdout
	if isTerminal(os.Stdout) && os.Getenv("QF_LOG_PRETTY") != "false" {
		out = zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}
	}

	return zerolog.New(out).
		Level(lvl).
		With().
		Timestamp().
		Str("service", service).
		Logger()
}

func isTerminal(f *os.File) bool {
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}
