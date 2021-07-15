/*
This file contains an implementation of a type (CentroidManager) which is
intended to manage common.Centroid; it could be thought of as a 'KMeans'
type, though that naming can mislead the intent, which is simply to manage
groups of centroids.
*/
package centoidmanager

import (
	"trypo/pkg/kmeans/common"
	"trypo/pkg/searchutils"
)

// Named parameter funcs. See NewCentroidManagerArgs.KNNSearchFunc.
type vecGenerator = func() ([]float64, bool)
type knnSearchFunc = func(targetVec []float64, vecs vecGenerator, k int) []int

// Centroid T in kmeans context. Implements common.Centroid interface.
type CentroidManager struct {
	vec []float64

	// Capacity of internal Centroids slice.
	initCap int

	// See NewCentroidManagerArgs.CentroidFactoryFunc.
	centroidFactoryFunc func(vec []float64) common.Centroid
	Centroids           []common.Centroid

	// This value specifies when internal Centroids should be split.
	// Specifically, if the amount of common.DataPoints in a centroid
	// exceeds this var, then it will be split in two. It is intended
	// to prevent overgrowth of Centroids. Note; having this at default
	// will disable the behavior.
	centroidDPThreshold int

	// See NewCentroidManagerArgs.KNNSearchFunc.
	knnSearchFunc knnSearchFunc
	// See NewCentroidManagerArgs.KFNSearchFunc.
	kfnSearchFunc knnSearchFunc
}

type NewCentroidManagerArgs struct {
	InitVec []float64
	// Capacity of internal Centroids slice.
	InitCap int

	// Specifies how to create new Centroids. The CentroidManager type doesn't
	// have any methods to directly add Centroids, that will be done as needed
	// automatically with this func.
	CentroidFactoryFunc func(vec []float64) common.Centroid

	CentroidDPThreshold int
	// KNNSearchFunc will be used for operations where it is necessary to find
	// k-nearest-neighs internally in a centroid; for instance with
	// DistributePayload when removing datapoints/payloads from self and then
	// calculating which receivers are best-fit (nearest) the removed datapoints.
	// The search func must be in the following form:
	//
	// 	Let vecGenerator = func() ([]float64, bool)
	// 	Let knnSearchFunc = func(targetVec []float64, vecs vecGenerator, k int) []int.
	//
	// vecGenerator is defined as an argument to knnSearchFunc, and should
	// be a generator that returns a []float64 representing a vector, and
	// a bool which signals stop. If this is unclear, see payloadVecGenerator() in
	// this file.
	//
	// The knnSearchFunc, as mentioned above, is expected to compare 'targetVec'
	// to all vectors generated by 'vecs' to find 'k' best-fit neighs, using
	// some kind of similarity/distance function (Cosine similarity, for instance).
	// The return is expected to be a slice of indeces referring to vectors
	// from 'vecs'. A known implementation of this, at the moment of writing,
	// is found in trypo/searchutils/knn.go (KNNCos & KNNEuc).
	KNNSearchFunc knnSearchFunc
	// KFNSearchFunc is the same as KNNSearchFunc but should find k furthest
	// neighs as opposed to nearest.
	KFNSearchFunc knnSearchFunc
}

// NewCentroid creates a new centroid manager with the specified args.
func NewCentroidManager(args NewCentroidManagerArgs) (*CentroidManager, bool) {
	if args.KNNSearchFunc == nil || args.KFNSearchFunc == nil {
		return nil, false
	}
	if args.CentroidFactoryFunc == nil {
		return nil, false
	}
	if args.CentroidFactoryFunc([]float64{1}) == nil {
		return nil, false
	}

	cm := CentroidManager{
		vec:                 make([]float64, len(args.InitVec)),
		centroidFactoryFunc: args.CentroidFactoryFunc,
		Centroids:           make([]common.Centroid, 0, args.InitCap),
		centroidDPThreshold: args.CentroidDPThreshold,
		initCap:             args.InitCap,
		knnSearchFunc:       args.KNNSearchFunc,
		kfnSearchFunc:       args.KFNSearchFunc,
	}
	for i, v := range args.InitVec {
		cm.vec[i] = v
	}
	return &cm, true
}

// centroidDataPointPortions creates a map where keys represent indexes into
// cm.Centroids, while vals represent a number of common.DataPoint for each
// key (those vals can't be higher than the actual amount of common.DataPoint
// contained for each centroid). The map is created such that Sum(vals) is as
// close to 'n' as possible, and the distribution of all vals in the map should
// be uniform. These conditions aren't guaranteed to be satisfied, as the amount
// of common.DataPoint contained in each centroid is uncertain.
func (cm *CentroidManager) centroidDataPointPortions(n int) map[int]int {
	portions := make(map[int]int)
	// This is essentially the total sum of all v in portions.
	assigned := 0
	for {
		// Keeps track of whether or not this loop made any change.
		assignedLast := assigned
		for i, centroid := range cm.Centroids {
			v, _ := portions[i]
			// Assign another portion if there are enough datapoints
			// for the centroid with this index.
			if centroid.LenDP() >= v+1 {
				portions[i] = v + 1
				assigned++
			}
			// Optimal case.
			if assigned >= n {
				return portions
			}
		}
		// Tried to assign but couldn't, no point in infinite looping.
		if assignedLast == assigned {
			break
		}
	}
	// Failed case: 'n' is higher than the total sum of payloadContainers
	return portions
}

// centroidVecGenerator returns a generator func which interates through all
// internal cm.Centroids and returns their vec.
func (cm *CentroidManager) centroidVecGenerator() func() ([]float64, bool) {
	i := 0
	return func() ([]float64, bool) {
		if i >= len(cm.Centroids) {
			return nil, false
		}
		i++
		return cm.Centroids[i-1].Vec(), true
	}
}

// splitCentroid 'splits' a centroid in cm.Centroids at the specified index.
// A new common.Centroid will be created with the same internal vector, and
// it will receive 'trimN' (max) common.DataPoints from the old Centroid.
func (cm *CentroidManager) splitCentroid(atIndex, trimN int) (common.Centroid, bool) {
	// Note; trimN <= 0 is important, as it ignores auto-splits done in
	// km.AddpayloadContainer when km.CentroidDPThreshold is not set (i.e 0).
	if atIndex < 0 || atIndex >= len(cm.Centroids) || trimN <= 0 {
		return nil, false
	}
	newCentroid := cm.centroidFactoryFunc(cm.Centroids[atIndex].Vec())
	dps := cm.Centroids[atIndex].DrainUnordered(trimN)
	for i := 0; i < len(dps); i++ {
		newCentroid.AddDataPoint(dps[i])
	}
	return newCentroid, true
}

// Vec exposes the internal vector of a CentroidManager.
func (km *CentroidManager) Vec() []float64 { return km.vec }

// AddDataPoint adds a common.DataPoint to a CentroidManager. It is potentially
// an expensive operation, as the dp will be put into a best-fit internal
// centroid. Best-fit will depend on the val given to
// NewCentroidManagerArgs.KNNSearchFunc while calling NewCentroidManager(...).
// Faild (return false) conditions:
// - dp expired.
// - Implementation issue of the aforementioned search func.
// - Vector of dp doesn't match any internal Centroid vec (unequal dimension).
// Note, this method auto-handles expired auto-splitting of internal
// Centroid slice.
func (cm *CentroidManager) AddDataPoint(dp common.DataPoint) bool {
	if dp.Expired() {
		return false
	}

	// Add first centroid.
	if len(cm.Centroids) == 0 {
		c := cm.centroidFactoryFunc(dp.Vec())
		c.AddDataPoint(dp)
		cm.Centroids = append(cm.Centroids, c)
		return true
	}

	// Try add to existing centroid.
	indexes := searchutils.KNNEuc(dp.Vec(), cm.centroidVecGenerator(), 1)
	if len(indexes) == 0 {
		return false
	}
	addOK := cm.Centroids[indexes[0]].AddDataPoint(dp)
	// Check add and whether or not to split the relevant centroid.
	if addOK && cm.Centroids[indexes[0]].LenDP() >= cm.centroidDPThreshold {
		newCentroid, splitOK := cm.splitCentroid(indexes[0], cm.centroidDPThreshold/2)
		if splitOK {
			cm.Centroids = append(cm.Centroids, newCentroid)
		}
	}
	return addOK
}

// DrainUnordered drains max n datapoints from internal centroids in an order
// that has no particular _intended_ significance, depending on how internal
// Centroids (created with NewCentroidManagerArgs.CentroidFactoryFunc, set
// when using NewCentroidManager(...)) implement the method with the same name.
// The draining load will be as uniform/even as possible amongst the internal
// centroids, i.e if n=2 and there are 2 centroids with at least 1 dp each, then
// both of them will drain 1 dp.
func (cm *CentroidManager) DrainUnordered(n int) []common.DataPoint {
	res := make([]common.DataPoint, 0, n)
	for centroidIndex, portion := range cm.centroidDataPointPortions(n) {
		res = append(res, cm.Centroids[centroidIndex].DrainUnordered(portion)...)
	}
	return res
}

// DrainOrdered drains max n 'worst-fit' datapoints from internal centroids,
// depending on how Centroids (created with CentroidFactoryFunc field specified
// when using NewCentroidManagerArgs for NewCentroidManager(...)) implement
// the method with the same name. The draining load will be as uniform/even as
// possible amongst the internal centroids, i.e if n=2 and there are 2 centroids
// with at least 1 dp each, then both of them will drain 1 dp.
func (cm *CentroidManager) DrainOrdered(n int) []common.DataPoint {
	res := make([]common.DataPoint, 0, n)
	for centroidIndex, portion := range cm.centroidDataPointPortions(n) {
		res = append(res, cm.Centroids[centroidIndex].DrainOrdered(portion)...)
	}
	return res
}
