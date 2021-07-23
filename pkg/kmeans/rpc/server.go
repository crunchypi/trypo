package rpc

import (
	"time"
	"trypo/pkg/kmeans/common"
)

// DataPoint is a concrete impl of the common.DataPoint iface.
// It has slightly odd namings, specifically prefixed with a
// capital I (for ignore), simply to ignore 'gob: type rpc.DataPoint
// has no exported fields' and 'other declaration of ...'.
type DataPoint struct {
	IVec           []float64
	IPayload       []byte
	IExpires       time.Time
	IExpireEnabled bool
}

func (dp *DataPoint) Vec() []float64 { return dp.IVec }

func (dp *DataPoint) Payload() []byte { return dp.IPayload }

func (dp *DataPoint) Expired() bool {
	return dp.IExpireEnabled && time.Now().After(dp.IExpires)
}

func (s *KMeansServer) Vec(namespace string, resp *[]float64) error {
	lookupOK := s.Table.Access(namespace, func(cm common.CentroidManager) {
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
	lookupOK := s.Table.Access(args.NameSpace, func(cm common.CentroidManager) {
		*resp = cm.AddDataPoint(&args.DP)
	})
	// Namespace doesn't exist, create one + add dp there.
	if !lookupOK {
		centroidManager := s.CentroidManagerFactoryFunc(args.DP.Vec())
		centroidManager.AddDataPoint(&args.DP)
		slot := CManagerSlot{cManager: centroidManager}
		// Returns a false if a slot is the containec CentroidManager is
		// nil, but it is assumed that it works here.
		s.Table.AddSlot(args.NameSpace, &slot)
	}

	*resp = true
	return nil
}
