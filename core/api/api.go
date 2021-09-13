/*
The api package defines a JSON/POST API for the system, using std net/http.
See routes in ./handler.go.

*/
package api

import (
	"net/http"
	"time"
	"trypo/pkg/arbiter"
)

// Alias for readability.
type Addr = arbiter.Addr

// APIConfig is used as args to the Start func.
type APIConfig struct {
	// Addr specifies the address of the server.
	Addr Addr

	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// Start starts a http.Server which is intended to be used to interface the trypo system.
func Start(cfg APIConfig) error {
	h := handler{}
	h.setRoutes()

	s := http.Server{
		Addr:         cfg.Addr.ToStr(),
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}

	return s.ListenAndServe()
}
