/*

Server for RPC layer on top of pkg/kmeans/centroidmanager.


*/
package rpc

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
