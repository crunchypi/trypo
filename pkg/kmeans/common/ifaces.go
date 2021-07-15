/*
Although this is an antipattern in Go, interfaces common for this project are
moved here, as the 'implicit' interface implementation isn't always implicit.


-------------------------------------------------------------------------------
Why:

Some methods in the kmeans pkg add/return an interface representation of a
type, where those interfaces were defined in the same pkg. Using those
methods from outside, however, required the specific interfaces in the kmeans
pkg, i.e implicit but _not_ between packages in this case. Example:

"
kmeansns.Centroid does not implement Centroid (wrong type for
AddDataPoint method)
have	AddDataPoint(kmeans.DataPoint) error
want	AddDataPoint(DataPoint) error
"

-------------------------------------------------------------------------------
What:

A few interfaces descriptive of elements useful for kmeans, specifically;
- DataPoint
- Centroid
- CentroidManager

*/
package common

type VecContainer interface {
	Vec() []float64
}

// DataPoint is a container of a vec, some payload and an expiration time.
type DataPoint interface {
	VecContainer
	Payload() []byte
	Expired() bool
}

// DataPointReceiver receives DataPoints.
type DataPointReceiver interface {
	VecContainer
	AddDataPoint(dp DataPoint) bool
}

// DataPointDrainer drains itself of- and returns payloads.
type DataPointDrainer interface {
	DrainUnordered(n int) []DataPoint
	DrainOrdered(n int) []DataPoint
}

// DataPointExpirer expires and drops datapoints.
type DataPointExpirer interface {
	Expire()
}

// MemTrimmer does a potentially costly mem reduction operation.
type MemTrimmer interface {
	MemTrim()
}

// VecMover moves one or more internal vectors in a meaningful way.
// This could be to sort itself for more accurate lookup results
// (see KNNSearcher in this file).
type VecMover interface {
	MoveVector() bool
}

// DataPointDistributer drains itself of- and distributes datapoints to receivers.
type DataPointDistributer interface {
	DistributeDataPoints(n int, receivers []DataPointReceiver)
}

// KNNSearcher does a KNN payload lookup using vectors.
type KNNSearcher interface {
	KNNLookup(vec []float64, k int, drain bool) []DataPoint
}

// Centroid is a composite interface which is intended to keep/manage datapoints
// as a centroid in the context of this pkg. It breaks the naming convention
// for clarity reasons.
type Centroid interface {
	DataPointReceiver
	DataPointDrainer
	DataPointExpirer
	MemTrimmer
	VecMover
	DataPointDistributer
	KNNSearcher
	LenDP() int
}
