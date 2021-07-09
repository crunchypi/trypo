/*
File contains a datapoint type that is common to this project.
*/
package common

import "time"

// Iface hint:
var _ PayloadContainer = new(DataPoint)

type DataPoint struct {
	vec           []float64
	payload       []byte
	expire        time.Time
	expireEnabled bool
}

// NewDataPointConfig is solely for the NewDataPoint func. This is used
// to create a new DataPoint which needs its fields unexported such that
// it can meet some interface requirements..............................
type NewDataPointConfig struct {
	Vec           []float64
	Payload       []byte
	Expire        time.Time
	ExpireEnabled bool
}

// NewDataPoint uses NewDataPointConfig to create a new DataPoint.
func NewDataPoint(cfg NewDataPointConfig) DataPoint {
	return DataPoint{
		vec:           cfg.Vec,
		payload:       cfg.Payload,
		expire:        cfg.Expire,
		expireEnabled: cfg.ExpireEnabled,
	}
}

// Vec gives the internal vector of a datapoint.
func (dp *DataPoint) Vec() []float64 {
	return dp.vec
}

// Payload gives the internal payload of a datapoint.
func (dp *DataPoint) Payload() []byte {
	return dp.payload
}

// Expires returns true if the datapoint is considered ot be expired.
func (dp *DataPoint) Expired() bool {
	return dp.expireEnabled && time.Now().After(dp.expire)
}
