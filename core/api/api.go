/*
The api package defines a JSON/POST API for the system, using std net/http.
See routes in ./handler.go.

*/
package api

import (
	"errors"
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

	// RPCAddrs should contain all addresses used in RPC network which contains
	// all the nodes which handle the data used in this system and service (i.e
	// approximate nearest neighs search). Should contain addr for local rpc
	// instance, not to be confused with the Addr field of this struct.
	RPCAddrs []Addr
}

func (cfg *APIConfig) check() error {
	if cfg.RPCAddrs == nil {
		return errors.New("unexpected nil for RPCAddrs field in APIConfig")
	}
	return nil
}

// Start starts a http.Server which is intended to be used to interface the trypo system.
func Start(cfg APIConfig) error {
	if err := cfg.check(); err != nil {
		return err
	}

	h := handler{RPCAddrs: cfg.RPCAddrs}
	h.setRoutes()

	s := http.Server{
		Addr:         cfg.Addr.ToStr(),
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}

	return s.ListenAndServe()
}
