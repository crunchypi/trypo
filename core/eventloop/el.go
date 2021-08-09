package eventloop

import (
	"time"
	"trypo/pkg/arbiter"

	"github.com/crunchypi/go-narb/apsa/rpc"
)

type Addr = arbiter.Addr

type eventLoopInternal struct {
	stopped bool
	iter    int
}

type EventLoopConfig struct {
	LocalAddr   Addr
	RemoteAddrs []Addr

	TimeoutLoop time.Duration
	TimeoutStep time.Duration

	// Added by event loop.
	internal eventLoopInternal
}

// This is intended to be a thing that is called for each event-loop task
// that does some network orchestration, so there will be only one node
// in the network that does the 'task',
func eventLoopStep(cfg *EventLoopConfig, task func(*EventLoopConfig)) {
	// For quickly aborting all steps in eventloop.
	if cfg.internal.stopped {
		return
	}
	time.Sleep(cfg.TimeoutStep)

	// Neat in case timeout is long.
	if cfg.internal.stopped {
		return
	}

	// Only do task if local is arbiter.
	client := arbiter.ArbiterClient(cfg.LocalAddr, nil)
	arbiterData := client.Arbiter()

	// Something wrong, try to set global arbiter.
	if arbiterData.Status != rpc.StatusOK {
		clients := arbiter.ArbiterClients(cfg.RemoteAddrs, nil, nil)
		ok := clients.TryForceNewArbiter(100)
		if !ok {
			panic("couldn't get arbiter consensus in event loop step")
		}
		// Retry getting arbiter.
		arbiterData = client.Arbiter()
	}

	// Local is simply not arbiter.
	if !arbiterData.Addr.Comp(cfg.LocalAddr) {
		return
	}
	// Check again in case the arbiter resolution takes a while.
	if cfg.internal.stopped {
		return
	}

	task(cfg)
}

func EventLoop(cfg *EventLoopConfig) func() {
	if cfg == nil {
		panic("nil eventloop cfg")
	}

	go func() {
		// Note: tasks are _not_ arbitrarily ordered.
		for cfg.internal.stopped == false {
			time.Sleep(cfg.TimeoutLoop)

			// Tick & wraparound.
			cfg.internal.iter++
			if cfg.internal.iter > 1000 {
				cfg.internal.iter = 0
			}
		}
	}()

	return func() {
		cfg.internal.stopped = true
	}
}
