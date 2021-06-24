package kmeans

import "time"

type DataPoint struct {
	Vec           []float64
	Payload       []byte
	Expire        time.Time
	ExpireEnabled bool
}

func (dp *DataPoint) Expired() bool {
	return dp.ExpireEnabled && time.Now().After(dp.Expire)
}

type DataPointAdder interface {
	AddDataPoint(dp DataPoint) error
}

type DataPointDrainer interface {
	DrainUnordered(n int) []DataPoint
	DrainOrdered(n int) []DataPoint
}

type VecContainer interface {
	Vec() []float64
}

type DataPointExpirer interface {
	ExpireDataPoints()
}

type DataPointContainer interface {
	DataPointAdder
	DataPointDrainer
	DataPointExpirer
	LenDP() int
}

type VecMover interface {
	VecContainer
	MoveVector() bool
}

type MemTrimmer interface {
	MemTrim()
}

type DataPointDistributer interface {
	DistributeDataPoints(n int, receivers []interface {
		VecContainer
		DataPointAdder
	})
}

type DataPointSearcher interface {
	KNNDataPointLookupCos(vec []float64, k int, drain bool) []DataPoint
}
