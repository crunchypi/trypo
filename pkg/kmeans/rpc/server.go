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
