package kmeans

import (
	"errors"
	"trypo/pkg/mathutils"
	"trypo/pkg/searchutils"
)

type KMeans struct {
	Centroids []interface {
		DataPointContainer
		VecContainer
		VecMover
		DataPointDistributer
		DataPointSearcher
		MemTrimmer
	}
	// @ Having this as default will disable any splits.
	CentroidDPThreshold int
	vec                 []float64
}

func newCentroidSlice(size, capacity int) []interface {
	DataPointContainer
	VecContainer
	VecMover
	DataPointDistributer
	DataPointSearcher
	MemTrimmer
} {
	if size < 0 {
		size = 0
	}
	if capacity < size {
		capacity = size
	}
	return make([]interface {
		DataPointContainer
		VecContainer
		VecMover
		DataPointDistributer
		DataPointSearcher
		MemTrimmer
	}, size, capacity)

}

func NewKMeansFromVec(vec []float64, CentroidDPThreshold int) *KMeans {
	return &KMeans{
		Centroids:           newCentroidSlice(0, 10),
		vec:                 vec,
		CentroidDPThreshold: CentroidDPThreshold,
	}
}

func (km *KMeans) Vec() []float64 { return km.vec }

func (km *KMeans) centroidVecGenerator() func() ([]float64, bool) {
	i := 0
	return func() ([]float64, bool) {
		if i >= len(km.Centroids) {
			return nil, false
		}
		i++
		return km.Centroids[i-1].Vec(), true
	}
}

func (km *KMeans) splitCentroid(centroidAtIndex, trimN int) *Centroid {
	// Note; trimN <= 0 is important, as it ignores auto-splits done in
	// km.AddDataPoint when km.CentroidDPThreshold is not set (i.e 0).
	if centroidAtIndex < 0 || centroidAtIndex >= len(km.Centroids) || trimN <= 0 {
		return nil
	}
	newCentroid := NewCentroidFromVec(km.Centroids[centroidAtIndex].Vec())
	for i := 0; i < trimN; i++ {
		dp := km.Centroids[centroidAtIndex].DrainUnordered(1)
		if len(dp) != 0 {
			newCentroid.AddDataPoint(dp[0])
		}
	}
	return newCentroid
}

// centroidDataPointPortions creates a map where keys represent indexes into
// km.Centroids, while vals represent a number of DataPoints for each key
// (those vals can't be higher than the actual amount of DataPoints contained
// for each centroid). The map is created such that Sum(vals) is as close to
// 'n' as possible, and the distribution of all vals in the map should be
// uniform. These conditions aren't guaranteed to be satisfied, as the amount
// of DataPoints contained in each centroid is uncertain.
func (km *KMeans) centroidDataPointPortions(n int) map[int]int {
	portions := make(map[int]int)
	// This is essentially the total sum of all v in portions.
	assigned := 0
	for {
		// Keeps track of whether or not this loop made any change.
		assignedLast := assigned
		for i, centroid := range km.Centroids {
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
	// Failed case: 'n' is higher than the total sum of DataPoints
	return portions
}

func (km *KMeans) AddDataPoint(dp DataPoint) error {
	if dp.Expired() {
		return errors.New("meh1") // @
	}

	if len(km.Centroids) == 0 {
		c := NewCentroidFromVec(dp.Vec)
		c.AddDataPoint(dp)
		km.Centroids = append(km.Centroids, c)
		return nil
	}

	indexes := searchutils.KNNEuc(dp.Vec, km.centroidVecGenerator(), 1)
	if len(indexes) == 0 {
		return errors.New("meh") // @
	}
	addErr := km.Centroids[indexes[0]].AddDataPoint(dp)
	if addErr == nil && km.Centroids[indexes[0]].LenDP() >= km.CentroidDPThreshold {
		newCentroid := km.splitCentroid(indexes[0], km.CentroidDPThreshold)
		if newCentroid != nil {
			km.Centroids = append(km.Centroids, newCentroid)
		}
	}

	return addErr
}

func (km *KMeans) DrainUnordered(n int) []DataPoint {
	res := make([]DataPoint, 0, n)
	for centroidIndex, portion := range km.centroidDataPointPortions(n) {
		res = append(res, km.Centroids[centroidIndex].DrainUnordered(portion)...)
	}
	return res
}

func (km *KMeans) DrainOrdered(n int) []DataPoint {
	res := make([]DataPoint, 0, n)
	for centroidIndex, portion := range km.centroidDataPointPortions(n) {
		res = append(res, km.Centroids[centroidIndex].DrainOrdered(portion)...)
	}
	return res
}

func (km *KMeans) ExpireDataPoints() {
	for _, centroid := range km.Centroids {
		centroid.ExpireDataPoints()
	}
}

func (km *KMeans) LenDP() int {
	res := 0
	for _, centroid := range km.Centroids {
		res += centroid.LenDP()
	}
	return res
}

func (km *KMeans) MemTrim() {
	centroids := newCentroidSlice(0, len(km.Centroids))
	for _, centroid := range km.Centroids {
		centroid.MemTrim()
		if centroid.LenDP() != 0 {
			centroids = append(centroids, centroid)
		}
	}
	km.Centroids = centroids
}

func (km *KMeans) MoveVector() bool {
	for _, centroid := range km.Centroids {
		centroid.MoveVector()
	}

	vec, ok := mathutils.VecMean(km.centroidVecGenerator())
	if ok {
		km.vec = vec
	}
	return ok
}

func (km *KMeans) Split(centroidDPThreshold int) {
	// @ This needs a rethink.
	newCentroids := newCentroidSlice(0, 10)

	for i, centroid := range km.Centroids {
		if centroid.LenDP() >= centroidDPThreshold {
			trim := centroid.LenDP() / 2
			newCentroids = append(newCentroids, km.splitCentroid(i, trim))
		}
	}
	km.Centroids = append(km.Centroids, newCentroids...)
}

func (km *KMeans) DistributeDataPoints(n int, receivers []interface {
	VecContainer
	DataPointAdder
}) {
	// use km.Centroids if recievers is not set.
	if receivers == nil {
		receivers = make([]interface {
			VecContainer
			DataPointAdder
		}, len(km.Centroids))
		for i, centroid := range km.Centroids {
			receivers[i] = centroid
		}
	}
	for centroidIndex, portion := range km.centroidDataPointPortions(n) {
		km.Centroids[centroidIndex].DistributeDataPoints(portion, receivers)
	}
}
