package eventloop

import (
	"fmt"
	"trypo/pkg/kmeans/rpc"
)

// Wrapper for handing event-loop-task skipping.
func withSkip(cfg *EventLoopConfig, interval int, task func()) {
	if cfg.internal.iter%interval == 0 {
		task()
	}
}

// Wrapper which creates an addrNamespaceTable. The intended usage is to group
// addresses (as a slice of Addr) by namespaces (so a map where keys are namespaces
// and vals are slices of Addr). This is helpful when moving data between nodes,
// for instance, because namespaces group data together logically.
func withNamespaceTable(cfg *EventLoopConfig, task func(addrNamespaceTable)) {
	table := addrNamespaceTable{}
	for _, addr := range cfg.RemoteAddrs {
		namespaces := rpc.KMeansClient(addr.ToStr(), "", nil).Namespaces()
		for _, namespace := range namespaces {
			table.addEntry(addr, namespace)
		}
	}
	task(table)
}

// Wrapper that iterates over local addr and all relevant namespaces.
func withLocalAddrNamespaces(cfg *EventLoopConfig, task func(addr Addr, namespace string)) {
	namespaces := rpc.KMeansClient(cfg.LocalAddr.ToStr(), "", nil).Namespaces()
	for _, ns := range namespaces {
		task(cfg.LocalAddr, ns)
	}
}

// Event-loop task for triggering the 'expire' procedure for the local addr (
// for all namespaces).
func eltExpire(cfg *EventLoopConfig) {
	withSkip(cfg, cfg.TaskSkip.Expire, func() {
		withLocalAddrNamespaces(cfg, func(addr Addr, namespace string) {
			s := "[%v (ns '%v')] | task: expire"
			cfg.L.LogTask(fmt.Sprintf(s, addr.ToStr(), namespace))

			go rpc.KMeansClient(addr.ToStr(), namespace, nil).Expire()
		})
	})
}

// Event-loop task for triggering the 'memtrim' procedure for the local addr (
// for all namespaces).
func eltMemTrim(cfg *EventLoopConfig) {
	withSkip(cfg, cfg.TaskSkip.MemTrim, func() {
		withLocalAddrNamespaces(cfg, func(addr Addr, namespace string) {
			s := "[%v (ns '%v')] | task: memtrim"
			cfg.L.LogTask(fmt.Sprintf(s, addr.ToStr(), namespace))

			go rpc.KMeansClient(addr.ToStr(), namespace, nil).MemTrim()
		})
	})
}

// Event-loop task for triggering the 'distribute datapoints (fast variant)'
// procedure, from local addr/node to all remotes (for all namespaces).
func eltDistributeDataPointsFast(cfg *EventLoopConfig) {
	withSkip(cfg, cfg.TaskSkip.DistributeDataPointsFast, func() {
		withLocalAddrNamespaces(cfg, func(addr Addr, namespace string) {
			withNamespaceTable(cfg, func(table addrNamespaceTable) {
				s := "[%v (ns '%v')] | task: distri fast"
				cfg.L.LogTask(fmt.Sprintf(s, addr.ToStr(), namespace))

				// Addrs for this local namespace.
				addrs := make([]string, len(table.items[namespace]))
				for i, addr := range table.items[namespace] {
					addrs[i] = addr.ToStr()
				}
				client := rpc.KMeansClient(addr.ToStr(), namespace, nil)
				n := cfg.DistributeDataPointsFastN
				client.DistributeDataPointsFast(addrs, n)

			})
		})
	})
}

// Event-loop task for triggering the 'distribute datapoints (accurate variant)'
// procedure, from local addr/node to all remotes (for all namespaces).
func eltDistributeDataPointsAccurate(cfg *EventLoopConfig) {
	withSkip(cfg, cfg.TaskSkip.DistributeDataPointsAccurate, func() {
		withLocalAddrNamespaces(cfg, func(addr Addr, namespace string) {
			withNamespaceTable(cfg, func(table addrNamespaceTable) {
				s := "[%v (ns '%v')] | task: distri accurate"
				cfg.L.LogTask(fmt.Sprintf(s, addr.ToStr(), namespace))

				// Addrs for this local namespace.
				addrs := make([]string, len(table.items[namespace]))
				for i, addr := range table.items[namespace] {
					addrs[i] = addr.ToStr()
				}
				client := rpc.KMeansClient(addr.ToStr(), namespace, nil)
				n := cfg.DistributeDataPointsAccurateN
				client.DistributeDataPointsAccurate(addrs, n)

			})
		})
	})
}

// Event-loop task for triggering the 'distribute datapoints (internal variant)'
// procedure for the local addr/node (all namespaces).
func eltDistributeDataPointsInternal(cfg *EventLoopConfig) {
	withSkip(cfg, cfg.TaskSkip.DistributeDataPointsInternal, func() {
		withLocalAddrNamespaces(cfg, func(addr Addr, namespace string) {
			s := "[%v (ns '%v')] | task: distri internal"
			cfg.L.LogTask(fmt.Sprintf(s, addr.ToStr(), namespace))

			client := rpc.KMeansClient(addr.ToStr(), namespace, nil)
			n := cfg.DistributeDataPointsInternalN
			go client.DistributeDataPointsInternal(n)
		})
	})
}

// Event-loop task for triggering the 'split centroids' procedure for the local
// addr (for all namespaces).
func eltSplitCentroids(cfg *EventLoopConfig) {
	withSkip(cfg, cfg.TaskSkip.SplitCentroids, func() {
		withLocalAddrNamespaces(cfg, func(addr Addr, namespace string) {
			s := "[%v (ns '%v')] | task: splitting"
			cfg.L.LogTask(fmt.Sprintf(s, addr.ToStr(), namespace))

			client := rpc.KMeansClient(addr.ToStr(), namespace, nil)
			go client.SplitCentroids(cfg.SplitCentroidsMin, cfg.SplitCentroidsMax)
		})
	})
}

// Event-loop task for triggering the 'merge centroids' procedure for the local
// addr (for all namespaces).
func eltMergeCentroids(cfg *EventLoopConfig) {
	withSkip(cfg, cfg.TaskSkip.MergeCentroids, func() {
		withLocalAddrNamespaces(cfg, func(addr Addr, namespace string) {
			s := "[%v (ns '%v')] | task: merging"
			cfg.L.LogTask(fmt.Sprintf(s, addr.ToStr(), namespace))

			client := rpc.KMeansClient(addr.ToStr(), namespace, nil)
			go client.MergeCentroids(cfg.MergeCentroidsMin, cfg.MergeCentroidsMax)
		})
	})
}

// Event-loop task for load balancing (from remotes to local node). It tries
// to transfer _whole_ Centroids from remote nodes to local node, based on
// the mean/average amount of DPs globally (so not necassarily based on
// byte amounts). The condition for transferring is if the local node has
// a below-average amount of dps, and the receiver has an above-average
// amount of dps -- even after loosing the sent data.
func eltLoadBalancing(cfg *EventLoopConfig) {
	withSkip(cfg, cfg.TaskSkip.LoadBalancing, func() {
		withLocalAddrNamespaces(cfg, func(addr Addr, namespace string) {
			withNamespaceTable(cfg, func(table addrNamespaceTable) {
				addrs := table.addrsWithNamespace(namespace)
				addrsLens := fetchRemoteLenDPs(addrs, namespace)

				dpTotal := 0
				for _, dpLen := range addrsLens {
					dpTotal += dpLen
				}
				dpMean := dpTotal / len(addrsLens)

				client := rpc.KMeansClient(addr.ToStr(), namespace, nil)
				for other, otherLen := range addrsLens {
					if addrsLens[addr] > dpMean {
						return
					}

					// '/n len(addrs)' for attempted even distribution.
					transferDPN := (dpMean - addrsLens[addr]) / len(addrs)
					// No point in transferring if this would put 'other' below
					// mean. 'addr' and 'other' comparison isn't strictly needed
					// due to the arithmetic but still, juuuust in case...
					if addr.Comp(other) || otherLen-transferDPN < dpMean {
						continue
					}

					n, _ := client.StealCentroids(other.ToStr(), transferDPN)

					s := "[%v (ns '%v')] | task: load balancing -> %v (n=%v)"
					s = fmt.Sprintf(s, addr.ToStr(), namespace, other.ToStr(), n)
					cfg.L.LogTask(s)

					// Update table.
					addrsLens[addr] = addrsLens[addr] + n
					addrsLens[other] = addrsLens[other] - n

				}

			})
		})
	})
}
