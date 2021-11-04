package arbiter

import (
	"time"

	"github.com/crunchypi/go-narb/apsa/common"
	"github.com/crunchypi/go-narb/apsa/rpc"
	"github.com/crunchypi/go-narb/apsa/sessionmember"
)

// Abbreviations.
type Addr = common.Addr
type SessionMember = sessionmember.SessionMember
type ArbiterServer = rpc.ArbiterServer
type StatusCode = common.StatusCode

// Abbreviation for func that sets up clients (orchestration of arbiterClient)
var ArbiterClients = rpc.ArbiterClients
var ArbiterClient = rpc.ArbiterClient

// NewSessionMemberConfig is a slightly simplified version of
// sessionmember.NewSessionMemberConfig, where the 'F' field
// is omitted (has a default in NewSessionMemer func in this pkg).
type NewSessionMemberConfig struct {
	LocalAddr       Addr
	Whitelist       []Addr
	SessionDuration time.Duration
	ArbiterDuration time.Duration
}

// NewSessionMember creates an arbitration sessionmember.
func NewSessionMember(cfg NewSessionMemberConfig) *SessionMember {
	f := sessionmember.Funcs{RemoteVoteFunc: rpc.RemoteVoteFunc()}
	cfg2 := sessionmember.NewSessionMemberConfig{
		LocalAddr:       cfg.LocalAddr,
		Whitelist:       cfg.Whitelist,
		F:               f,
		SessionDuration: cfg.SessionDuration,
		ArbiterDuration: cfg.ArbiterDuration,
	}

	return sessionmember.NewSessionMember(cfg2)
}

// Convenient 'errors' arg for ArbiterClients func.
type ArbErrs map[Addr]error

// Convenient 'statuses' arg for ArbiterClients func.
type ArbStats map[Addr]StatusCode

// CheckAll checks all errors in the ArbErrs map. The first != nil that
// is found will be returned with the relevant Addr. If no errors exist
// then the returns is an empty Addr and a nil
func (e *ArbErrs) CheckAll() (Addr, error) {
	for addr, err := range *e {
		if err != nil {
			return addr, err
		}
	}
	return Addr{}, nil
}

// UniformStatus returns true if all status codes in ArbStats are sc.
func (s *ArbStats) UniformStatus(sc StatusCode) bool {
	for _, status := range *s {
		if status != sc {
			return false
		}
	}
	return true
}

// NewArbiterServer creates an ArbiterServer (RPC layer on top of SessionMember).
func NewArbiterServer(cfg NewSessionMemberConfig) *ArbiterServer {
	return rpc.NewArbiterServer(NewSessionMember(cfg))
}
