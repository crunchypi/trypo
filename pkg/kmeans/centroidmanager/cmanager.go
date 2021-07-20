/*
This file contains an implementation of a type (CentroidManager) which is
intended to manage common.Centroid; it could be thought of as a 'KMeans'
type, though that naming can mislead the intent, which is simply to manage
groups of centroids.
*/
package centoidmanager

import (
	"trypo/pkg/kmeans/common"
	"trypo/pkg/mathutils"
)

// Interface hint:
var _ common.Centroid = new(CentroidManager)

// Abbreviation.
type dpReceivers = []common.DataPointReceiver

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
	// km.AddDataPoint when km.CentroidDPThreshold is not set (i.e 0).
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

// prepVecUpdate prepares a task for updating the internal cm.vec. It takes
// an 'old' vector, and returns a func which accepts a 'new' vector. When
// calling that func, the internal cm.vec will update cm.vec such that
// the 'old' vector is replaced with the 'new' vector internally. An
// example to clarity:
//	A method of CentroidManager wants to change a centroid in cm.Centroids
//	such that it's internal vector is changed (by removing dps, for example).
//	cm.vec (which represents the mean of all centroids in
//  cm.Centroids, or their vecs, to be specific) will now be inaccurate
//	and an option is to call CentroidManager.MoveVector(). That is expensive,
//	as it is O(n) ish, where n is len(cm.Centroids) and calling it every time
//	something changes is bad. Instead, one can use this method like this:
//
//	centroidToChange := ...
//	prepUpdate := prepVecUpdate(centroidToChange.Vec())
//	... do something to centroidToChange that changes it's vec.
//	prepUpdate(centroidToChange.Vec())
//
// Note, this does not support deleting centroids.
func (cm *CentroidManager) prepVecUpdate(v1 []float64) func([]float64) {
	oldVec := make([]float64, (len(v1)))
	copy(oldVec, v1)
	return func(v2 []float64) {
		cmLen := float64(len(cm.Centroids))
		cm.vec = mathutils.VecMulScalar(cm.vec, cmLen)
		cm.vec, _ = mathutils.VecSub(cm.vec, oldVec)
		cm.vec, _ = mathutils.VecAdd(cm.vec, v2)
		cm.vec = mathutils.VecDivScalar(cm.vec, cmLen)
	}
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
// Centroid slice, as well as auto-updating the internal vec of CentroidManager.
func (cm *CentroidManager) AddDataPoint(dp common.DataPoint) bool {
	if dp.Expired() {
		return false
	}

	// Add first centroid.
	if len(cm.Centroids) == 0 {
		c := cm.centroidFactoryFunc(dp.Vec())
		c.AddDataPoint(dp)
		cm.Centroids = append(cm.Centroids, c)
		cm.vec = c.Vec()
		return true
	}

	// Try find nearest centroid.
	indexes := cm.knnSearchFunc(dp.Vec(), cm.centroidVecGenerator(), 1)
	if len(indexes) == 0 {
		return false
	}

	// Try add to nearest centroid.
	centroid := cm.Centroids[indexes[0]]          // Abbreviation.
	updateVec := cm.prepVecUpdate(centroid.Vec()) // Track old vec.
	if !centroid.AddDataPoint(dp) {
		return false
	}

	// Adjust cm.vec.
	updateVec(centroid.Vec())

	// Potential centroid split.
	if centroid.LenDP() >= cm.centroidDPThreshold {
		newCentroid, splitOK := cm.splitCentroid(indexes[0], cm.centroidDPThreshold/2)
		if splitOK {
			cm.Centroids = append(cm.Centroids, newCentroid)
		}
	}
	return true
}

// DrainUnordered drains max n datapoints from internal centroids in an order
// that has no particular _intended_ significance, depending on how internal
// Centroids (created with NewCentroidManagerArgs.CentroidFactoryFunc, set
// when using NewCentroidManager(...)) implement the method with the same name.
// The draining load will be as uniform/even as possible amongst the internal
// centroids, i.e if n=2 and there are 2 centroids with at least 1 dp each, then
// both of them will drain 1 dp. Note, will update CentroidManager vector.
func (cm *CentroidManager) DrainUnordered(n int) []common.DataPoint {
	res := make([]common.DataPoint, 0, n)
	for centroidIndex, portion := range cm.centroidDataPointPortions(n) {
		centroid := cm.Centroids[centroidIndex]
		// Prep for internal vec update.
		updateVec := cm.prepVecUpdate(centroid.Vec())
		res = append(res, centroid.DrainUnordered(portion)...)
		// Finalize internal vec update.
		updateVec(centroid.Vec())

	}
	return res
}

// DrainOrdered drains max n 'worst-fit' datapoints from internal centroids,
// depending on how Centroids (created with CentroidFactoryFunc field specified
// when using NewCentroidManagerArgs for NewCentroidManager(...)) implement
// the method with the same name. The draining load will be as uniform/even as
// possible amongst the internal centroids, i.e if n=2 and there are 2 centroids
// with at least 1 dp each, then both of them will drain 1 dp. Note, will
// update CentroidManager vector.
func (cm *CentroidManager) DrainOrdered(n int) []common.DataPoint {
	res := make([]common.DataPoint, 0, n)
	for centroidIndex, portion := range cm.centroidDataPointPortions(n) {
		centroid := cm.Centroids[centroidIndex]
		// Prep for internal vec update.
		updateVec := cm.prepVecUpdate(centroid.Vec())
		res = append(res, centroid.DrainOrdered(portion)...)
		// Finalize internal vec update.
		updateVec(centroid.Vec())
	}
	return res
}

// Expire calls the method with the same name on all internal Centroids.
// This should expire all datapoints stored in all centroids, but the exact
// behavior depends on the implementation of Centroids returned with the
// func specified when creating this CentroidManager instance (see
// NewCentroidManagerArgs.CentroidFactoryFunc).
// Note, will update CentroidManager vector.
func (cm *CentroidManager) Expire() {
	for _, centroid := range cm.Centroids {
		// Prep for internal vec update.
		updateVec := cm.prepVecUpdate(centroid.Vec())
		centroid.Expire()
		// Finalize internal vec update.
		updateVec(centroid.Vec())
	}
}

// LenDP calls the method with the same name on all internal Centroids,
// and returns the sum of their returns. This should return the total amount
// of DataPoints stored in this instance, but the exact behavior depends on
// the implementation of Centroids returned with the func specified when
// creating this CentroidManager instance (see CentroidFactoryFunc filed of
// NewCentroidManagerArgs).
func (cm *CentroidManager) LenDP() int {
	res := 0
	for _, centroid := range cm.Centroids {
		res += centroid.LenDP()
	}
	return res
}

// MemTrim resets the internal Centroid slice where empty Centroids are
// not included (so cap=len). Note, will update CentroidManager vector.
func (cm *CentroidManager) MemTrim() {
	centroids := make([]common.Centroid, 0, len(cm.Centroids))
	for _, centroid := range cm.Centroids {
		// Prep for internal vec update.
		updateVec := cm.prepVecUpdate(centroid.Vec())
		centroid.MemTrim()
		// Finalize internal vec update.
		updateVec(centroid.Vec())
		if centroid.LenDP() != 0 {
			centroids = append(centroids, centroid)
		}

		// Will not be included in new centroids slice, so effectively deleted.
		// In this case, the internal vec must be updated a bit differently
		// than what cm.prepVecUpdate does. Note bounds check.
		if centroid.LenDP() == 1 && len(cm.Centroids) > 1 {
			cm.vec = mathutils.VecMulScalar(cm.vec, float64(len(cm.Centroids)))
			cm.vec, _ = mathutils.VecSub(cm.vec, centroid.Vec())
			cm.vec = mathutils.VecDivScalar(cm.vec, float64(len(cm.Centroids)-1))
		}
	}
	cm.Centroids = centroids
}

// MoveVector sets the internal vector to the average of all internal Centroids.
func (cm *CentroidManager) MoveVector() bool {
	for _, centroid := range cm.Centroids {
		centroid.MoveVector()
	}

	vec, ok := mathutils.VecMean(cm.centroidVecGenerator())
	if ok {
		cm.vec = vec
	}
	return ok
}

// DistributeDataPoints should drain 'n' DataPoints from internal Centroids and
// give them to the best-fit 'receivers'. The exact behavior, i.e draining and
// finding 'best-fit' will depend on how the internal Centroids (created with
// CentroidFactoryFunc field specified when using NewCentroidManagerArgs for
// NewCentroidManager(...)) imlement the method with the same name. Additionally,
// 'n' will be divided as evenly/uniformly as possible amongst the internal
// Centroids (so if n=2 and there are 2 centroids with at least 1 dp each, then
// both of them will give away 1 dp each). Finally, receivers=nil will change
// the behavior of this func such that all internal Centroids are receivers.
// This should in practice distribute datapoints amongst best-fit internal
// Centroids. Note, will update internal CentroidManager vector.
func (cm *CentroidManager) DistributeDataPoints(n int, receivers dpReceivers) {
	// use km.Centroids if recievers is not set.
	if receivers == nil {
		receivers = make([]common.DataPointReceiver, len(cm.Centroids))
		for i, centroid := range cm.Centroids {
			receivers[i] = centroid
		}
	}
	for centroidIndex, portion := range cm.centroidDataPointPortions(n) {
		cm.Centroids[centroidIndex].DistributeDataPoints(portion, receivers)
	}
	// Done in bulk here, as opposed to using cm.prepVecUpdate, because
	// it's uncertain which receiver(s) will be picked when calling
	// Centroid.DistributeDataPoints,
	cm.MoveVector()
}

// KNNLookup should find 'k' datapoints that are 'nearest' the 'vec' arg and
// also remove them from internal storage if 'drain'=true. To be specific,
// it will find internal Centroids that are best-fit to 'vec', which will
// depend on how the func specified in NewCentroidManagerArgs.KNNSearchFunc
// works (this could be cosine similarity, for instance) -- then the method
// with the same name (KNNLookup) will be called on those Centroids (that
// behavor depends on how Centroids are created with the CentroidFactoryFunc
// func, specified when using NewCentroidManagerArgs for NewCentroidManager()).
// Note, will update internal CentroidManager vector.
func (cm *CentroidManager) KNNLookup(vec []float64, k int, drain bool) []common.DataPoint {
	res := make([]common.DataPoint, 0, k)

	gen := cm.centroidVecGenerator() // Brevity.
	for _, centroidIndex := range cm.knnSearchFunc(vec, gen, k) {
		if len(res) >= k {
			break
		}

		centroid := cm.Centroids[centroidIndex]
		// Prep for internal vec update.
		updateVec := cm.prepVecUpdate(centroid.Vec())
		for _, dp := range centroid.KNNLookup(vec, k-len(res), drain) {
			switch {
			// Keep adding to res until requirement is met.
			case len(res) < k:
				res = append(res, dp)

			// Requirement met and drain is on, so the dps iterated over
			// in this loop have to be added somewhere (back into centroid).
			case len(res) >= k && drain:
				centroid.AddDataPoint(dp)

			// Requirement met and drain is off, so no dps will be lost
			// if a break is done here.
			case len(res) >= k && !drain:
				break
			}
		}
		// Finalize internal vec update.
		updateVec(centroid.Vec())
	}
	return res
}

// NearestCentroid attempts to find a Centroid that is 'nearest' the specified
// vec; returns false if there are no internal Centroids, or if none of them
// have a matching vector (different vector dim). 'nearest' will depend on how
// the CentroidManager search func works (specified as KNNSearchFunc field in
// NewCentroidManagerArgs when using NewCentroidManager(...)). NOTE; if the
// internal datapoint state of the returned centroid is changed, do a call
// to CentroidManager.MoveVector() to update the internal vec.
func (cm *CentroidManager) NearestCentroid(vec []float64) (common.Centroid, bool) {
	indexes := cm.knnSearchFunc(vec, cm.centroidVecGenerator(), 1)
	if len(indexes) == 0 {
		return nil, false
	}
	return cm.Centroids[indexes[0]], true
}

// SplitCentroids iterates through all internal Centroids and passes them to
// the evaluation func 'splits' -- if it returns true, then that centroid will
// be split in half. Example:
//	split := func(c common.Centroid) { return c.LenDP() > 100 }
// .. will split in half all centroids that have more than 100 internal DPs.
func (cm *CentroidManager) SplitCentroids(split func(common.Centroid) bool) {
	newCentroids := make([]common.Centroid, 0, 10)

	for i, centroid := range cm.Centroids {
		if !split(centroid) {
			continue
		}

		newCentroid, splitOK := cm.splitCentroid(i, centroid.LenDP()/2)
		if splitOK {
			newCentroids = append(newCentroids, newCentroid)
		}
	}
	cm.Centroids = append(cm.Centroids, newCentroids...)
}

// MergeCentroids iterates through all internal Centroids and passes them to
// the evaluation func 'merge' -- if it returns true, then that centroid will
// be merged to the nearest other Centroids until the evaluation func is
// satisfied (eval func is not done on the aforementioned other Centroids).
// 'nearest' will depend on how the CentroidManager search func works (specifie
// as KNNSearchFunc field in NewCentroidManagerArgs when using NewCentroidManager()).
// Example 1:
//	merge := func(c common.Centroid) { return true }
//	... will merge all centroids together.
//	Example 2:
//	merge := func(c common.Centroid) { return c.LenDP() < 10 }
//	... will find all centroids that have less than 10 dps, then merge them
//	into their _nearest_ other centroids until they have at least 10 dps.
//
// Note, will update internal CentroidManager vector.
func (cm *CentroidManager) MergeCentroids(merge func(common.Centroid) bool) {
	// When two centroids are merged, one of them will be marked for deletion
	// here, to prevent duplication of internal data. Keys=cm.Centroids indexes.
	delMarks := make(map[int]bool, len(cm.Centroids))
	for i, candidate := range cm.Centroids {
		if !merge(candidate) || delMarks[i] {
			continue
		}
		// Abbreviations:
		vec := candidate.Vec()
		gen := cm.centroidVecGenerator()
		k := len(cm.Centroids)

		// Prep for internal vec update for 'candidate' centroid.
		updateVecCandidate := cm.prepVecUpdate(vec)

		// Merge nearest centroids into 'candidate' until merge()=true.
		for _, centroidIndex := range cm.knnSearchFunc(vec, gen, k) {
			// Guard identity or double merge.
			if centroidIndex == i || delMarks[centroidIndex] {
				continue
			}
			// Merge other _completely_ into candidate.
			delMarks[centroidIndex] = true
			other := cm.Centroids[centroidIndex]
			for _, dp := range other.DrainUnordered(other.LenDP()) {
				candidate.AddDataPoint(dp)
			}
			if !merge(candidate) { // Check if satisfied.
				break
			}
		}
		// Finalize internal vec update for 'candidate' centroid.
		updateVecCandidate(candidate.Vec())
	}
	// Filter out centroids marked for deletion from cm.Centroids.
	// Note, the reason for backwards looping is to prevent index
	// shifting issues.
	for i := len(cm.Centroids) - 1; i > -1; i-- {
		if delMarks[i] {
			// This should always be true, due to how this func works at the
			// time of writing, but doing a bounds check juuust in case:
			if len(cm.Centroids) > 0 {
				// Need this, it changes the internal vector appropriately
				// when deleting a centroid:
				cm.vec = mathutils.VecMulScalar(cm.vec, float64(len(cm.Centroids)))
				cm.vec, _ = mathutils.VecSub(cm.vec, cm.Centroids[i].Vec())
				cm.vec = mathutils.VecDivScalar(cm.vec, float64(len(cm.Centroids)-1))
			}
			// Delete.
			cm.Centroids = append(cm.Centroids[:i], cm.Centroids[i+1:]...)
		}
	}
}

// DangerExposeCentroidSlice simply exposes internal slice of Centroids.
// Altering this slice might be dangerous/lead to internal state issues.
func (cm *CentroidManager) DangerExposeCentroidSlice() []common.Centroid {
	return cm.Centroids
}
