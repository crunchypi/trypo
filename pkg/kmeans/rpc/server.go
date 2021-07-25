package rpc

func (s *KMeansServer) Vec(namespace string, resp *[]float64) error {
	lookupOK := s.Table.Access(namespace, func(cm *CentroidManager) {
		*resp = cm.Vec()
	})

	if !lookupOK {
		return NamespaceErr{namespace}
	}
	return nil
}

type AddDataPointArgs struct {
	NameSpace string
	DP        DataPoint
}

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

type drainF = func(*CentroidManager) []DataPoint

func (s *KMeansServer) drain(ns string, resp *[]DataPoint, f drainF) error {
	lookupOK := s.Table.Access(ns, func(cm *CentroidManager) {
		*resp = f(cm)
	})
	if !lookupOK {
		return NamespaceErr{ns}
	}
	return nil
}

func (s *KMeansServer) DrainUnordered(args DrainArgs, resp *[]DataPoint) error {
	return s.drain(args.NameSpace, resp, func(cm *CentroidManager) []DataPoint {
		return cm.DrainUnordered(args.N)
	})
}

func (s *KMeansServer) DrainOrdered(args DrainArgs, resp *[]DataPoint) error {
	return s.drain(args.NameSpace, resp, func(cm *CentroidManager) []DataPoint {
		return cm.DrainOrdered(args.N)
	})
}

func (s *KMeansServer) Expire(namespace string, _ *int) error {
	lookupOK := s.Table.Access(namespace, func(cm *CentroidManager) {
		cm.Expire()
	})
	if !lookupOK {
		return NamespaceErr{namespace}
	}
	return nil
}

func (s *KMeansServer) LenDP(namespace string, resp *int) error {
	lookupOK := s.Table.Access(namespace, func(cm *CentroidManager) {
		*resp = cm.LenDP()
	})
	if !lookupOK {
		return NamespaceErr{namespace}
	}
	return nil
}

func (s *KMeansServer) MemTrim(namespace string, _ *int) error {
	lookupOK := s.Table.Access(namespace, func(cm *CentroidManager) {
		cm.MemTrim()
	})
	if !lookupOK {
		return NamespaceErr{namespace}
	}
	return nil
}

func (s *KMeansServer) MoveVector(namespace string, resp *bool) error {
	lookupOK := s.Table.Access(namespace, func(cm *CentroidManager) {
		*resp = cm.MoveVector()
	})
	if !lookupOK {
		return NamespaceErr{namespace}
	}
	return nil
}

/*
Reason for why DistributeDataPoints method is missing is written in client.go,
right after MoveVector.
*/

type DistribDPIArgs struct {
	NameSpace string
	N         int
}

func (s *KMeansServer) DistributeDataPointsInternal(args DistribDPIArgs, _ *int) error {
	lookupOK := s.Table.Access(args.NameSpace, func(cm *CentroidManager) {
		cm.DistributeDataPointsInternal(args.N)
	})
	if !lookupOK {
		return NamespaceErr{args.NameSpace}
	}
	return nil
}

type KNNLookupArgs struct {
	NameSpace string
	Vec       []float64
	K         int
	Drain     bool
}

func (s *KMeansServer) KNNLookup(args KNNLookupArgs, resp *[]DataPoint) error {
	lookupOK := s.Table.Access(args.NameSpace, func(cm *CentroidManager) {
		*resp = cm.KNNLookup(args.Vec, args.K, args.Drain)
	})
	if !lookupOK {
		return NamespaceErr{args.NameSpace}
	}
	return nil
}

type NearestCentroidArgs struct {
	NameSpace string
	Vec       []float64
	N         int
	Drain     bool
}

func (s *KMeansServer) NearestCentroids(args NearestCentroidArgs, r *[]*Centroid) error {
	lookupOK := s.Table.Access(args.NameSpace, func(cm *CentroidManager) {
		*r, _ = cm.NearestCentroids(args.Vec, args.N, args.Drain)
	})
	if !lookupOK {
		return NamespaceErr{args.NameSpace}
	}
	return nil
}

type NearestCentroidVecArgs struct {
	NameSpace string
	Vec       []float64
}

func (s *KMeansServer) NearestCentroidVec(args NearestCentroidVecArgs, r *[]float64) error {
	lookupOK := s.Table.Access(args.NameSpace, func(cm *CentroidManager) {
		centroids, _ := cm.NearestCentroids(args.Vec, 1, false)
		if len(centroids) != 0 {
			*r = centroids[0].Vec()
		}
	})
	if !lookupOK {
		return NamespaceErr{args.NameSpace}
	}
	return nil
}

type SplitCentroidsArgs struct {
	NameSpace  string
	DPRangeMin int
	DPRangeMax int
}

func (s *KMeansServer) SplitCentroids(args SplitCentroidsArgs, _ *int) error {
	lookupOK := s.Table.Access(args.NameSpace, func(cm *CentroidManager) {
		cm.SplitCentroids(func(c *Centroid) bool {
			return c.LenDP() > args.DPRangeMin && c.LenDP() < args.DPRangeMax
		})
	})
	if !lookupOK {
		return NamespaceErr{args.NameSpace}
	}
	return nil
}

type MergeCentroidsArgs struct {
	NameSpace  string
	DPRangeMin int
	DPRangeMax int
}

func (s *KMeansServer) MergeCentroids(args SplitCentroidsArgs, _ *int) error {
	lookupOK := s.Table.Access(args.NameSpace, func(cm *CentroidManager) {
		cm.MergeCentroids(func(c *Centroid) bool {
			return c.LenDP() > args.DPRangeMin && c.LenDP() < args.DPRangeMax
		})
	})
	if !lookupOK {
		return NamespaceErr{args.NameSpace}
	}
	return nil
}
