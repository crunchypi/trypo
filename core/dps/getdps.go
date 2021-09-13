/*
See file comment in dps.go
*/
package dps

import (
	"trypo/core/nodes"
	"trypo/pkg/kmeans/rpc"
)

type getDataPointsArgs struct {
	addrOpts  []Addr
	namespace string
	queryVec  []float64
	n         int
	drain     bool
}

func getDataPoints(args getDataPointsArgs) []DataPoint {
	res := make([]DataPoint, 0, args.n)
	for _, addr := range args.addrOpts {
		client := rpc.KMeansClient(addr.ToStr(), args.namespace, nil)
		dps := client.KNNLookup(args.queryVec, args.n-len(res), args.drain)
		res = append(res, dps...)
		if len(res) >= args.n {
			break
		}
	}
	return res
}

type GetDataPointsArgs struct {
	// AddrOptions contains addresses of nodes to be considered.
	AddrOptions []Addr
	// Namespace for data.
	Namespace string
	// QueryVec is used for searching most similar datapoints.
	QueryVec []float64
	// N specifies how many dps to fetch.
	N int
	// Drain will remote dps that are fetched.
	Drain bool

	// KNNsearchFunc is used to find best-fit nodes to pull dps from.
	KNNSearchFunc knnSearchFunc
}

func (a *GetDataPointsArgs) toBestFitNodesArgs() nodes.BestFitNodesArgs {
	return nodes.BestFitNodesArgs{
		AddrOpts:      a.AddrOptions,
		Namespace:     a.Namespace,
		Vec:           a.QueryVec,
		KNNSearchFunc: a.KNNSearchFunc,
	}
}

func (a *GetDataPointsArgs) toPrivate(newAddrs []Addr) getDataPointsArgs {
	return getDataPointsArgs{
		addrOpts:  newAddrs,
		namespace: a.Namespace,
		queryVec:  a.QueryVec,
		n:         a.N,
		drain:     a.Drain,
	}
}

// GetDataPointsRand will fetch dps randomly from remote nodes. This does not
// require the 'KNNsearchFunc' field in 'args'.
func GetDataPointsRand(args GetDataPointsArgs) []DataPoint {
	addrs := shuffleAddrs(args.AddrOptions)
	return getDataPoints(args.toPrivate(addrs))
}

// GetDataPointsFast will fetch remote dps in haste with some accuracy.
// Specifically, it will find 'best-fit' node(s) using core/nodes.BestFitNodesFast()
// to fetch dps from.
func GetDataPointsFast(args GetDataPointsArgs) []DataPoint {
	addrs := nodes.BestFitNodesFast(args.toBestFitNodesArgs())
	return getDataPoints(args.toPrivate(addrs))
}

// GetDataPointsAccurate is similar to GetDataPointsFast but differs by finding
// 'best-fit' node(s) using core/nodes.BestFitNodesAccurate(), which is slower
// but would yield more accurate results.
func GetDataPointsAccurate(args GetDataPointsArgs) []DataPoint {
	addrs := nodes.BestFitNodesAccurate(args.toBestFitNodesArgs())
	return getDataPoints(args.toPrivate(addrs))
}
