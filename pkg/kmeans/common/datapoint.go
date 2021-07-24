package common

import "time"

// DataPoint is a common data carrier in this pkg.
type DataPoint struct {
	Vec           []float64
	Payload       []byte
	Expires       time.Time
	ExpireEnabled bool
}

// Expired returns true if dp.ExpireEnabled=true and dp.Expires
// is a time before now.
func (dp *DataPoint) Expired() bool {
	return dp.ExpireEnabled && time.Now().After(dp.Expires)
}

// DataPointReceiver receives DataPoints.
type DataPointReceiver interface {
	Vec() []float64
	AddDataPoint(dp DataPoint) bool
}
