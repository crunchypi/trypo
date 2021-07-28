package arbiter

import (
	"testing"

	"github.com/crunchypi/go-narb/apsa/common"
	"github.com/crunchypi/go-narb/apsa/rpc"
)

type network struct {
	nodes     map[common.Addr]*rpc.ArbiterServer
	stopFuncs map[common.Addr]func()
}

func newNetwork(addrs []common.Addr) *network {
	r := network{
		nodes:     make(map[common.Addr]*rpc.ArbiterServer, len(addrs)),
		stopFuncs: make(map[common.Addr]func(), len(addrs)),
	}
	for _, addr := range addrs {
		server := NewArbiterServer(addr, addrs)
		// Func to call for stopping the server.
		stopServerFunc, err := rpc.StartListen(server, addr)
		if err != nil {
			// ...
		}
		r.nodes[addr] = server
		r.stopFuncs[addr] = stopServerFunc

	}
	return &r
}

func (n *network) cleanup() {
	for _, stopFunc := range n.stopFuncs {
		stopFunc()
	}
}

func TestClientsTryForceNewArbiter(t *testing.T) {
	addrs := []common.Addr{
		{"localhost", "3000"},
		{"localhost", "3001"},
		{"localhost", "3002"},
	}

	// Start servers.
	n := newNetwork(addrs)
	defer n.cleanup()

	errors := make(ArbErrs)
	statuses := make(ArbStats)
	retries := 100
	ok := rpc.ArbiterClients(addrs, errors, statuses).TryForceNewArbiter(retries)

	if addr, err := errors.CheckAll(); err != nil {
		t.Fatalf("(tryforce) %v: %v", addr.ToStr(), err)
	}

	if !ok {
		t.Fatalf("failed consensus after %v tries", retries)
	}

	_, ok = rpc.ArbiterClients(addrs, errors, statuses).Arbiter()

	if addr, err := errors.CheckAll(); err != nil {
		t.Fatalf("(arbiter) %v: %v", addr.ToStr(), err)
	}

	if !ok {
		t.Fatalf("successful tryforce but network still disagreed on arbiter")
	}

}
