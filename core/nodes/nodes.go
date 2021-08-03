/*
This pkg is ultimately meant for getting nodes (or rather, their addresses) by
some criteria. At the moment of writing, nodes are pkg/kmeans/rpc, and the
criteria is similarity of nodes (i.e pkg/kmeans things) to a vector. To
give an example:

  There are some nodes that contains kmeans stuff (Centroid or CentroidManager)
  and it is necessary to know which nodes are most 'similar' to a vector.
  The API in this pkg will accept some addresses, a namespace, a vector, as well
  as a similarity function (see 'knnSearchFunc' and BestFitNodesArgs) which
  would make this possible in a convenient way -- it will do some network calls
  and then sort those aforementioned addresses by similarity.
*/
package nodes

import (
	"trypo/pkg/arbiter"
)

// Alias for arbiter.Addr.
type Addr = arbiter.Addr

// Readability alias: standard generator that returns vectors (false=stop).
type vecGenerator = func() ([]float64, bool)

// Readability alias: a func that finds 'k' nearest neighs (vecs) of 'targetVec'.
type knnSearchFunc = func(targetVec []float64, vecs vecGenerator, k int) []int

// BestFitNodesArgs is meant as arguments for func starting with 'BestFitNodes'.
type BestFitNodesArgs struct {
	// AddrOpts abbreviates address options.
	AddrOpts []Addr
	// Namespace for data in remote nodes.
	Namespace string
	// Vec is what AddrOptts is sorted by.
	Vec []float64
	// KNNSearchFunc does the sorting of AddrOpts by Vec.
	KNNSearchFunc knnSearchFunc
}

func bestFitNodes(args BestFitNodesArgs, fetcher func() *fetchVecsChan) []Addr {
	res := make([]Addr, 0, len(args.AddrOpts))
	fch := fetcher()
	rsp := fch.collect()
	gen := rsp.intoVecGenerator()

	args.AddrOpts = rsp.intoAddrs() // Correlate order with 'gen'.
	for _, index := range args.KNNSearchFunc(args.Vec, gen, len(args.AddrOpts)) {
		res = append(res, args.AddrOpts[index])
	}
	return res
}

// BestFitNodesFast will sort args.AddrOpts by args.Vec as fast (and with as
// much accuracy as logically) possible. It will do calls to remote nodes
// (with specified addresses) keeping pkg/kmeans/centroidmanager.CentroidManager
// instances, get their vecs with pkg/kmeans/rpc.KMeansClient(..).Vec(), and
// then sort the nodes by those vecs and args.Vec.
func BestFitNodesFast(args BestFitNodesArgs) []Addr {
	return bestFitNodes(args, func() *fetchVecsChan {
		return fetchVecsFast(args.AddrOpts, args.Namespace)
	})
}

// BestFitNodesAccurate will sort args.AddrOpts by args.Vec with as much
// accuracy as possible, sacrificing haste. It will do calls to remote nodes
// (with specified addresses) keeping pkg/kmeans/centroidmanager.CentroidManager
// instances. What makes this different than BestFitNodes (of this pkg) is that
// the args.Vec will be compared to Centroid instances in CentroidManager, i.e
// a greater detail level. The vecs (of Centroid, not CentroidManager instances)
// will be used to sort args.AddrOpts.
func BestFitNodesAccurate(args BestFitNodesArgs) []Addr {
	return bestFitNodes(args, func() *fetchVecsChan {
		return fetchVecsAccurate(args.AddrOpts, args.Namespace, args.Vec)
	})
}
