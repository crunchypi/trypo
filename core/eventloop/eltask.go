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
			cfg.L.LogTask(fmt.Sprintf("(ns '%v') expire", namespace))

			go rpc.KMeansClient(addr.ToStr(), namespace, nil).Expire()
		})
	})
}

// Event-loop task for triggering the 'memtrim' procedure for the local addr (
// for all namespaces).
func eltMemTrim(cfg *EventLoopConfig) {
	withSkip(cfg, cfg.TaskSkip.MemTrim, func() {
		withLocalAddrNamespaces(cfg, func(addr Addr, namespace string) {
			cfg.L.LogTask(fmt.Sprintf("(ns '%v') memtrim", namespace))

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
				cfg.L.LogTask(fmt.Sprintf("(ns '%v') distri fast", namespace))

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
				cfg.L.LogTask(fmt.Sprintf("(ns '%v') distri accurate", namespace))

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
			cfg.L.LogTask(fmt.Sprintf("(ns '%v') distri internal", namespace))

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
			cfg.L.LogTask(fmt.Sprintf("(ns '%v') splitting", namespace))

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
			cfg.L.LogTask(fmt.Sprintf("(ns '%v') merging", namespace))

			client := rpc.KMeansClient(addr.ToStr(), namespace, nil)
			go client.MergeCentroids(cfg.MergeCentroidsMin, cfg.MergeCentroidsMax)
		})
	})
}

// Event-loop task for load balancing (from local node to remotes). It tries
// to transfer _whole_ Centroids from remote nodes to local node, based on
// the mean/average amount of DPs globally (so not necassarily based on
// byte amounts). The condition for transferring is if the local node has
// a below-average amount of dps, and the receiver has an above-average
// amount of dps -- even after loosing the sent data.
func eltLoadBalancing(cfg *EventLoopConfig) {
	// NOTE: Some of the constants below, specifically 'margin' and
	// how 'transferDPN' is divided, are arbitrary but seem to work
	// after doing some experimentation (using the monitor in the
	// test file ./el_test.go).
	withSkip(cfg, cfg.TaskSkip.LoadBalancing, func() {
		withNamespaceTable(cfg, func(table addrNamespaceTable) {
			for _, ns := range table.namespaces() {
				addrs := table.addrsWithNamespace(ns)
				addrsLens := fetchRemoteLenDPs(addrs, ns)

				local := cfg.LocalAddr // Abbreviation.

				dpTotal := 0
				for _, dpLen := range addrsLens {
					dpTotal += dpLen
				}
				dpMean := dpTotal / len(addrsLens)
				// Prevent a situation where nodes always move data.
				margin := int(float64(dpMean) * 0.4)

				client := rpc.KMeansClient(local.ToStr(), ns, nil)

				for other, otherLen := range addrsLens {
					// For clarity; data flow goes only from other nodes to local node.
					if local.Comp(other) || addrsLens[local] > dpMean {
						continue
					}

					// No point in getting any data if local is above average.
					if addrsLens[local] > dpMean-margin {
						continue
					}

					// '/n len(cfg.RemoteAddrs)' for attempted even distribution.
					transferDPN := (dpMean - addrsLens[local]) / len(cfg.RemoteAddrs)
					// Transferring in even smaller steps, especially since
					// client.StealCetroids has a tendency to overshoot the
					// amount of dps transferred (since Centroids are sent whole).
					transferDPN /= 3

					if transferDPN == 0 {
						continue
					}

					// No point in transferring if this would put 'other' below
					// mean -- unless it's the only one that has anything for
					// that namespace.
					if otherLen-transferDPN < dpMean+margin && len(addrs) != 1 {
						continue
					}

					n, _ := client.StealCentroids(other.ToStr(), transferDPN)

					s := "(ns '%v') load balancing (want %v dps, got %v from %v)"
					s = fmt.Sprintf(s, ns, transferDPN, n, other.ToStr())
					cfg.L.LogTask(s)

					// Update table.
					addrsLens[local] = addrsLens[local] + n
					addrsLens[other] = addrsLens[other] - n
				}
			}
		})
	})
}

func eltMeta(cfg *EventLoopConfig) {
	withSkip(cfg, cfg.TaskSkip.Meta, func() {
		metaData := MetaData{Items: make(map[Addr]MetaDataItem)}
		pullFrom := make([]Addr, 0, len(cfg.RemoteAddrs))

		if cfg.LogLocalOnly {
			pullFrom = append(pullFrom, cfg.LocalAddr)
		} else {
			pullFrom = append(pullFrom, cfg.RemoteAddrs...)
		}

		for _, addr := range pullFrom {
			meta := rpc.KMeansClient(addr.ToStr(), "", nil).Meta()
			metaData.Items[addr] = MetaDataItem{
				LenDP:        meta.DPs,
				LenCentroids: meta.Centroids,
			}
		}
		cfg.L.LogMeta(metaData)
	})
}
