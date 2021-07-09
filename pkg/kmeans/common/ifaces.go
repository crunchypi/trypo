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
kmeansns.Centroid does not implement CentroidComposite (wrong type for
AddPayload method)
have	AddPayload(kmeans.PayloadContainer) error
want	AddPayload(PayloadContainer) error
"

-------------------------------------------------------------------------------
What:

A few interfaces that can are layered and are meant to be descriptive of:
- 'Payloads', or the smallest type of data (holding []byte data, etc).
- 'Centroid', in a kmeans context. Keeps payloads.


*/
package common

// VecContainer is whatever contains a vector.
type VecContainer interface {
	Vec() []float64
}

// PayloadContainer is whatever keeps payloads.
type PayloadContainer interface {
	VecContainer
	Payload() []byte
	Expired() bool
}

// PayloadReceiver receives payloads.
type PayloadReceiver interface {
	VecContainer
	AddPayload(pc PayloadContainer) bool
}

// PayloadDrainer drains itself of- and returns payloads.
type PayloadDrainer interface {
	DrainUnordered(n int) []PayloadContainer
	DrainOrdered(n int) []PayloadContainer
}

// PayloadExpirer expires as drops payloads.
type PayloadExpirer interface {
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

// PayloadDistributer drains itself of- and distributes payloads to receivers.
type PayloadDistributer interface {
	DistributePayload(n int, receivers []PayloadReceiver)
}

// KNNSearcher does a KNN payload lookup using vectors.
type KNNSearcher interface {
	KNNLookup(vec []float64, k int, drain bool) []PayloadContainer
}

// Centroid is a composite interface which is intended to keep/manage payloads
// as a centroid in the context of this repo. It breaks the naming convention
// for clarity reasons.
type Centroid interface {
	PayloadReceiver
	PayloadDrainer
	PayloadExpirer
	MemTrimmer
	VecMover
	PayloadDistributer
	KNNSearcher
}
