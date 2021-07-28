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

// NewSessionMember creates an arbitration sessionmember.
func NewSessionMember(localAddr Addr, allAddrs []Addr) *SessionMember {
	f := sessionmember.Funcs{RemoteVoteFunc: rpc.RemoteVoteFunc()}
	cfg := sessionmember.NewSessionMemberConfig{
		LocalAddr:       localAddr,
		Whitelist:       allAddrs,
		F:               f,
		SessionDuration: time.Second * 5,
		ArbiterDuration: time.Minute * 5,
	}

	return sessionmember.NewSessionMember(cfg)
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
func NewArbiterServer(localAddr Addr, allAddrs []Addr) *ArbiterServer {
	return rpc.NewArbiterServer(NewSessionMember(localAddr, allAddrs))
}
