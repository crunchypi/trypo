/*
See file comment in dps.go
*/
package dps

import (
	"trypo/core/nodes"
	"trypo/pkg/kmeans/rpc"
)

type PutDataPointArgs struct {
	// AddrOptions contains addresses of nodes to be considered.
	AddrOptions []Addr
	// Namespace for data.
	Namespace string
	// DataPoint to put.
	DataPoint DataPoint

	// KNNsearchFunc is used to find best-fit nodes to put dps in.
	KNNSearchFunc knnSearchFunc
}

func (a *PutDataPointArgs) toBestFitNodesArgs() nodes.BestFitNodesArgs {
	return nodes.BestFitNodesArgs{
		AddrOpts:      a.AddrOptions,
		Namespace:     a.Namespace,
		Vec:           a.DataPoint.Vec,
		KNNSearchFunc: a.KNNSearchFunc,
	}
}

func putDataPoint(addrOpt []Addr, namespace string, dp DataPoint) bool {
	for _, addr := range addrOpt {
		client := rpc.KMeansClient(addr.ToStr(), namespace, nil)
		ok := client.AddDataPoint(dp)
		if ok {
			return true
		}
	}
	return false
}

// PutDataPointRand will put a dp in a random node.
func PutDataPointRand(args PutDataPointArgs) bool {
	addrs := shuffleAddrs(args.AddrOptions)
	return putDataPoint(addrs, args.Namespace, args.DataPoint)
}

// PutDataPointFast will put a dp in a remote node with haste and some accuracy.
// Specifically, it will find a 'best-fit' node to put the dp into by using
// core/nodes.BestFitNodesFast(...).
func PutDataPointFast(args PutDataPointArgs) bool {
	addrs := nodes.BestFitNodesFast(args.toBestFitNodesArgs())
	return putDataPoint(addrs, args.Namespace, args.DataPoint)
}

// PutDataPointAccurate is similar to PutDataPointFast but differs by finding
// 'best-fit' nodes with core/nodes.BestFitNodesAccurate(..).
func PutDataPointAccurate(args PutDataPointArgs) bool {
	addrs := nodes.BestFitNodesAccurate(args.toBestFitNodesArgs())
	return putDataPoint(addrs, args.Namespace, args.DataPoint)
}
