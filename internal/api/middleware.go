package api

import (
	"net/http"
	"runtime/debug"
	"time"
)

// requestLogger logs every HTTP request with method, path, status, duration.
func (s *Server) requestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(ww, r)
		s.Logger.Info().
			Str("method", r.Method).
			Str("path", r.URL.Path).
			Int("status", ww.status).
			Dur("duration", time.Since(start)).
			Str("remote", r.RemoteAddr).
			Msg("http request")
	})
}

// recoverer turns panics in handlers into 500s instead of crashing the binary.
func (s *Server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				s.Logger.Error().
					Interface("panic", rec).
					Bytes("stack", debug.Stack()).
					Msg("panic in handler")
				writeError(w, http.StatusInternalServerError, "internal_error", "unexpected error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}
