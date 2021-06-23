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
