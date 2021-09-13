package eventloop

import (
	"time"
	"trypo/pkg/arbiter"
)

type Addr = arbiter.Addr

// Intended to be used for each event loop task in the event loop.
func elStep(cfg *EventLoopConfig, task func(*EventLoopConfig)) {
	// For quickly aborting all steps in eventloop.
	if cfg.internal.stopped {
		return
	}
	time.Sleep(cfg.TimeoutStep)

	// Neat in case timeout is long.
	if cfg.internal.stopped {
		return
	}
	task(cfg)
}

func EventLoop(cfg *EventLoopConfig) func() {
	cfg.validate()

	go func() {
		// Note: tasks are _not_ arbitrarily ordered.
		for cfg.internal.stopped == false {
			time.Sleep(cfg.TimeoutLoop)

			elStep(cfg, eltMeta)

			elStep(cfg, eltExpire)
			elStep(cfg, eltMemTrim)

			elStep(cfg, eltMergeCentroids)
			elStep(cfg, eltSplitCentroids)

			elStep(cfg, eltDistributeDataPointsInternal)
			elStep(cfg, eltDistributeDataPointsFast)
			elStep(cfg, eltDistributeDataPointsAccurate)

			elStep(cfg, eltLoadBalancing)

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
