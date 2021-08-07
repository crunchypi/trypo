/*

Server for RPC layer on top of pkg/kmeans/centroidmanager.


*/
package rpc

import "trypo/pkg/searchutils"

// Namespaces sends all namespaces stored in the server.
func (s *KMeansServer) Namespaces(_ int, resp *[]string) error {
	s.Table.Lock()
	defer s.Table.Unlock()

	r := make([]string, 0, 20) // 20 is arbitrary.
	for key := range s.Table.slots {
		r = append(r, key)
	}
	*resp = r
	return nil
}

// Reduces some boilerplate by doing the '!lookupOK { ... } return nil' thing.
func (s *KMeansServer) handleNamespaceErr(ns string, f func(*CentroidManager)) error {
	lookupOK := s.Table.Access(ns, f)
	if !lookupOK {
		return NamespaceErr{ns}
	}
	return nil
}

// Forward call to the method with the same name on an instance of CentroidManager
// (pkg kmeans/CentroidManager). Returns a NamespaceErr if the namespace doesn't
// lead to an instance.
func (s *KMeansServer) Vec(namespace string, resp *[]float64) error {
	return s.handleNamespaceErr(namespace, func(cm *CentroidManager) {
		*resp = cm.Vec()
	})
}

type AddDataPointArgs struct {
	NameSpace string
	DP        DataPoint
}

// Forward call to the method with the same name on an instance of CentroidManager
// (pkg kmeans/CentroidManager). Will createa a new CentroidManager instance if
// the namespace is not currently in use.
func (s *KMeansServer) AddDataPoint(args AddDataPointArgs, resp *bool) error {
	lookupOK := s.Table.Access(args.NameSpace, func(cm *CentroidManager) {
		*resp = cm.AddDataPoint(args.DP)
	})
	// Namespace doesn't exist, create one + add dp there.
	if !lookupOK {
		centroidManager := s.CentroidManagerFactoryFunc(args.DP.Vec)
		centroidManager.AddDataPoint(args.DP)
		slot := CManagerSlot{cManager: centroidManager}
		// Returns a false if a slot is the containec CentroidManager is
		// nil, but it is assumed that it works here.
		s.Table.AddSlot(args.NameSpace, &slot)
	}

	*resp = true
	return nil
}

type DrainArgs struct {
	NameSpace string
	N         int
	Ordered   bool
}

// Forward call to the method with the same name on an instance of CentroidManager
// (pkg kmeans/CentroidManager). Returns a NamespaceErr if the namespace doesn't
// lead to an instance.
func (s *KMeansServer) DrainUnordered(args DrainArgs, resp *[]DataPoint) error {
	return s.handleNamespaceErr(args.NameSpace, func(cm *CentroidManager) {
		*resp = cm.DrainUnordered(args.N)
	})
}

// Forward call to the method with the same name on an instance of CentroidManager
// (pkg kmeans/CentroidManager). Returns a NamespaceErr if the namespace doesn't
// lead to an instance.
func (s *KMeansServer) DrainOrdered(args DrainArgs, resp *[]DataPoint) error {
	return s.handleNamespaceErr(args.NameSpace, func(cm *CentroidManager) {
		*resp = cm.DrainOrdered(args.N)
	})
}

// Forward call to the method with the same name on an instance of CentroidManager
// (pkg kmeans/CentroidManager). Returns a NamespaceErr if the namespace doesn't
// lead to an instance.
func (s *KMeansServer) Expire(namespace string, _ *int) error {
	return s.handleNamespaceErr(namespace, func(cm *CentroidManager) {
		cm.Expire()
	})
}

// Forward call to the method with the same name on an instance of CentroidManager
// (pkg kmeans/CentroidManager). Returns a NamespaceErr if the namespace doesn't
// lead to an instance.
func (s *KMeansServer) LenDP(namespace string, resp *int) error {
	return s.handleNamespaceErr(namespace, func(cm *CentroidManager) {
		*resp = cm.LenDP()
	})
}

// Forward call to the method with the same name on an instance of CentroidManager
// (pkg kmeans/CentroidManager). Returns a NamespaceErr if the namespace doesn't
// lead to an instance.
func (s *KMeansServer) MemTrim(namespace string, _ *int) error {
	return s.handleNamespaceErr(namespace, func(cm *CentroidManager) {
		cm.MemTrim()
	})
}

// Forward call to the method with the same name on an instance of CentroidManager
// (pkg kmeans/CentroidManager). Returns a NamespaceErr if the namespace doesn't
// lead to an instance.
func (s *KMeansServer) MoveVector(namespace string, resp *bool) error {
	return s.handleNamespaceErr(namespace, func(cm *CentroidManager) {
		*resp = cm.MoveVector()
	})
}

type DistribDPArgs struct {
	NameSpace   string
	N           int
	AddrOptions []string
}

// Try adding dp to any addr in addrs. addrs will ordered by (indexed into) using
// gen and a knn search func (searchutils.KNNCos at the time of writing) so there
// is some 'best-fit' involved. Returns false if dp isn't added anywhere.
func distributeDP(dp DataPoint, gen vecGenerator, addrs []string, namespace string) bool {
	for _, index := range searchutils.KNNCos(dp.Vec, gen, len(addrs)) {
		client := KMeansClient(addrs[index], namespace, nil)
		if client.AddDataPoint(dp) {
			return true
		}
	}
	return false
}

// DistributeDataPointsFast will try to distribute args.N datapoints in haste
// (with some accuracy) from this node amongst 'best-fit' remote nodes listed
// in args.AddrOptions (all within the same args.NameSpace). Specifically, this
// node's CentroidManager with the given namespace will have its DrainOrdered
// method called, then those dps will be sent to remote nodes that are most
// similar to those dps (similarity is caluclated by remote CentroidManager.Vec()).
func (s *KMeansServer) DistributeDataPointsFast(args DistribDPArgs, _ *int) error {
	dps := make([]DataPoint, 0, args.N)

	// Not wrapping the code below with this because it locks the CentroidManager.
	nsErr := s.handleNamespaceErr(args.NameSpace, func(cm *CentroidManager) {
		dps = append(dps, cm.DrainOrdered(args.N)...)
	})

	if nsErr != nil || len(dps) == 0 || len(args.AddrOptions) == 0 {
		return nsErr
	}

	// Fetch remote vecs. Done outside of the dp loop (below) because these
	// vecs are not assumed to change by a lot (they might, if those nodes
	// have few dps, but the tradeoff is made nontheless).
	rch := fetchVecs(args.AddrOptions, func(addr string) ([]float64, bool) {
		var err error
		vec := KMeansClient(addr, args.NameSpace, &err).Vec()
		return vec, err == nil && vec != nil
	})
	rsl := rch.collect() // collect the chan.

	addrs := rsl.intoAddrs()
	for _, dp := range dps {
		gen := rsl.intoVecGenerator()
		if !distributeDP(dp, gen, addrs, args.NameSpace) {
			// Put back into self so the dp isn't lost.
			s.Table.Access(args.NameSpace, func(cm *CentroidManager) {
				cm.AddDataPoint(dp)
			})
		}
	}
	return nil
}

// DistributeDataPointsAccurate is similar to DistributeDataPointsFast but is
// slower and more accurate. The latter finds nodes that are most similar to
// the drained datapoints by using KMeansClient(...).Vec() _once_ for each
// address option, while this method uses KMeansClient(...).NearestCentroidVec(..)
// (slower and more accurate method) for each dp and for each address option.
// This is _a_lot_ slower due to many network calls, but has the benefit of
// placing distribute dps precisely.
func (s *KMeansServer) DistributeDataPointsAccurate(args DistribDPArgs, _ *int) error {
	dps := make([]DataPoint, 0, args.N)

	// Not wrapping the code below with this because it locks the CentroidManager.
	nsErr := s.handleNamespaceErr(args.NameSpace, func(cm *CentroidManager) {
		dps = append(dps, cm.DrainOrdered(args.N)...)
	})

	if nsErr != nil || len(dps) == 0 || len(args.AddrOptions) == 0 {
		return nsErr
	}

	for _, dp := range dps {
		// Fetch remote vecs. Done inside the loop, even though the dps might
		// not vary much with their vecs, because this method trades speed for
		// accuracy.
		rch := fetchVecs(args.AddrOptions, func(addr string) ([]float64, bool) {
			var err error
			vec := KMeansClient(addr, args.NameSpace, &err).NearestCentroidVec(dp.Vec)
			return vec, err == nil && vec != nil
		})
		rsl := rch.collect() // collect the chan.
		gen := rsl.intoVecGenerator()

		addrs := rsl.intoAddrs()
		if !distributeDP(dp, gen, addrs, args.NameSpace) {
			// Put back into self so the dp isn't lost.
			s.Table.Access(args.NameSpace, func(cm *CentroidManager) {
				cm.AddDataPoint(dp)
			})
		}
	}
	return nil
}

type DistribDPIArgs struct {
	NameSpace string
	N         int
}

// Forward call to the method with the same name on an instance of CentroidManager
// (pkg kmeans/CentroidManager). Returns a NamespaceErr if the namespace doesn't
// lead to an instance.
func (s *KMeansServer) DistributeDataPointsInternal(args DistribDPIArgs, _ *int) error {
	return s.handleNamespaceErr(args.NameSpace, func(cm *CentroidManager) {
		cm.DistributeDataPointsInternal(args.N)
	})
}

type KNNLookupArgs struct {
	NameSpace string
	Vec       []float64
	K         int
	Drain     bool
}

// Forward call to the method with the same name on an instance of CentroidManager
// (pkg kmeans/CentroidManager). Returns a NamespaceErr if the namespace doesn't
// lead to an instance.
func (s *KMeansServer) KNNLookup(args KNNLookupArgs, resp *[]DataPoint) error {
	return s.handleNamespaceErr(args.NameSpace, func(cm *CentroidManager) {
		*resp = cm.KNNLookup(args.Vec, args.K, args.Drain)
	})
}

type NearestCentroidArgs struct {
	NameSpace string
	Vec       []float64
	N         int
	Drain     bool
}

// Forward call to the method with the same name on an instance of CentroidManager
// (pkg kmeans/CentroidManager). Returns a NamespaceErr if the namespace doesn't
// lead to an instance.
func (s *KMeansServer) NearestCentroids(args NearestCentroidArgs, r *[]*Centroid) error {
	return s.handleNamespaceErr(args.NameSpace, func(cm *CentroidManager) {
		*r, _ = cm.NearestCentroids(args.Vec, args.N, args.Drain)
	})
}

type NearestCentroidVecArgs struct {
	NameSpace string
	Vec       []float64
}

// Forward call to the method with the same name on an instance of CentroidManager
// (pkg kmeans/CentroidManager). Returns a NamespaceErr if the namespace doesn't
// lead to an instance.
func (s *KMeansServer) NearestCentroidVec(args NearestCentroidVecArgs, r *[]float64) error {
	return s.handleNamespaceErr(args.NameSpace, func(cm *CentroidManager) {
		centroids, _ := cm.NearestCentroids(args.Vec, 1, false)
		if len(centroids) != 0 {
			*r = centroids[0].Vec()
		}
	})
}

type SplitCentroidsArgs struct {
	NameSpace  string
	DPRangeMin int
	DPRangeMax int
}

// Forward call to the method with the same name on an instance of CentroidManager
// (pkg kmeans/CentroidManager). Returns a NamespaceErr if the namespace doesn't
// lead to an instance.
func (s *KMeansServer) SplitCentroids(args SplitCentroidsArgs, _ *int) error {
	return s.handleNamespaceErr(args.NameSpace, func(cm *CentroidManager) {
		cm.SplitCentroids(func(c *Centroid) bool {
			return c.LenDP() > args.DPRangeMin && c.LenDP() < args.DPRangeMax
		})
	})
}

type MergeCentroidsArgs struct {
	NameSpace  string
	DPRangeMin int
	DPRangeMax int
}

// Forward call to the method with the same name on an instance of CentroidManager
// (pkg kmeans/CentroidManager). Returns a NamespaceErr if the namespace doesn't
// lead to an instance.
func (s *KMeansServer) MergeCentroids(args SplitCentroidsArgs, _ *int) error {
	return s.handleNamespaceErr(args.NameSpace, func(cm *CentroidManager) {
		cm.MergeCentroids(func(c *Centroid) bool {
			return c.LenDP() > args.DPRangeMin && c.LenDP() < args.DPRangeMax
		})
	})
}

type StealCentroidArgs struct {
	FromAddr  string
	NameSpace string
	// Will steal Centroids until the total DP amount exceeds this.
	TransferDPLimit int
}

type StealCentroidsResp struct {
	TransferredN int
	OK           bool
}

// StealCentroids will 'steal' one or more Centroid from a remote node, intended for
// load balancing. It will keep 'stealing' _whole_ Centroids until the total amount
// of datapoints exceeds args.TransferDPLimit (this value might therefore be greatly
// overshot), using KMeansClient().NearestCentroids(vec), where vec is the vector
// of CentroidManager for this node and namespace. The response 'r' will have
// different implied meanings:
//	- TransferredN = 0 & OK = false : remote node err (namespace or network issue).
//	- TransferredN > 0 & OK = false : Some Centroids transferred before network err.
//	- TransferredN = 0 & OK = true : No network err but remote is empty.
//	- TransferredN > 0 & OK = true : all ok.
func (s *KMeansServer) StealCentroid(args StealCentroidArgs, r *StealCentroidsResp) error {
	r.OK = true
	return s.handleNamespaceErr(args.NameSpace, func(cm *CentroidManager) {
		var err error
		client := KMeansClient(args.FromAddr, args.NameSpace, &err)
		for r.TransferredN < args.TransferDPLimit {
			// Can be improved. One at a time for convenience + readability.
			centroids, cOK := client.NearestCentroids(cm.Vec(), 1, true)

			if err != nil {
				r.OK = true
				return
			}
			if !cOK || len(centroids) == 0 {
				return
			}
			// @ unsafe, this is assuming cm has similar properties as centroids
			// @ (such as KNNSearchfunc, etc).
			cm.Centroids = append(cm.Centroids, centroids...)
			r.TransferredN += centroids[0].LenDP()
		}
	})
}
