package kmeans

import (
	"errors"
	"trypo/pkg/searchutils"
)

type Centroid struct {
	vec        []float64
	DataPoints []DataPoint
}

func NewCentroidFromVec(vec []float64) *Centroid {
	c := Centroid{
		vec:        make([]float64, len(vec)),
		DataPoints: make([]DataPoint, 0, 10), // 10 is arbitrary.
	}
	for i, v := range vec {
		c.vec[i] = v
	}
	return &c
}

func (c *Centroid) Vec() []float64 { return c.vec }

func (c *Centroid) AddDataPoint(dp DataPoint) error {
	if len(dp.Vec) != len(c.vec) {
		return errors.New("vector length of new datapoint is not supported in container")
	}
	if dp.Expired() {
		return nil
	}
	c.DataPoints = append(c.DataPoints, dp)
	return nil
}

func (c *Centroid) datapointVecGenerator() func() ([]float64, bool) {
	i := 0
	return func() ([]float64, bool) {
		// Skip expired datapoints.
		for {
			// First cond for preventing index error.
			if i < len(c.DataPoints) && c.DataPoints[i].Expired() {
				c.DataPoints = append(c.DataPoints[:i], c.DataPoints[i+1:]...)
				continue
			}
			break
		}
		if i >= len(c.DataPoints) {
			return nil, false
		}
		i++
		return c.DataPoints[i-1].Vec, true
	}
}

func (c *Centroid) DrainUnordered(n int) []DataPoint {
	res := make([]DataPoint, 0, n)
	// Odd looping because it's not known at this point how
	// many DataPoints are expired.
	for {
		// Exit clause: either looped through everything || result is satisfied.
		if len(c.DataPoints) == 0 || len(res) >= n {
			break
		}
		if !c.DataPoints[0].Expired() {
			res = append(res, c.DataPoints[0])
		}
		c.DataPoints = append(c.DataPoints[:0], c.DataPoints[1:]...)
	}
	return res
}

func (c *Centroid) DrainOrdered(n int) []DataPoint {
	res := make([]DataPoint, 0, n)
	indexes := searchutils.KFNEuc(c.vec, c.datapointVecGenerator(), n)
	for _, i := range indexes {
		res = append(res, c.DataPoints[i])
	}
	// Second loop for draining, as the vals in 'indexes' might not be ordered.
	for _, i := range indexes {
		c.DataPoints = append(c.DataPoints[:i], c.DataPoints[i+1:]...)
	}
	return res
}

func (c *Centroid) ExpireDataPoints() {
	// Odd looping because it's not known how many DataPoints are expired,
	// and so avoding an indes increment is helpful.
	i := 0
	for {
		if i >= len(c.DataPoints) {
			break
		}
		if c.DataPoints[i].Expired() {
			// Should be re-sliced with O(1). Needs confirmation, though.
			c.DataPoints = append(c.DataPoints[:i], c.DataPoints[i+1:]...)
			continue
		}
		i++
	}
}

func (c *Centroid) LenDP() int { return len(c.DataPoints) }

func (c *Centroid) MemTrim() {
	// @ Currently inefficient since memory is essentially doubled
	// @ while doing this procedure.
	dp := make([]DataPoint, 0, len(c.DataPoints))
	for i := 0; i < len(c.DataPoints); i++ {
		if !c.DataPoints[i].Expired() {
			dp = append(dp, c.DataPoints[i])
		}
	}
	c.DataPoints = dp
}

func (c *Centroid) MoveVector() bool {
	vec, ok := mathutils.VecMean(c.datapointVecGenerator())
	if ok {
		c.vec = vec
	}
	return ok
}

func (c *Centroid) DistributeDataPoints(n int, receivers []interface {
	VecContainer
	DataPointAdder
}) {
	// Need to have a slice here (i.e can't draw datapoints directly from
	// c.DataPoints) because this instance (c) can be one of the distributers.
	dp := c.DrainOrdered(n)
	i := 0
	generator := func() ([]float64, bool) {
		if i >= len(receivers) {
			return nil, false
		}
		i++
		return receivers[i-1].Vec(), true
	}

	for j := 0; j < len(dp); j++ {
		i = 0 // Reset generator.
		indexes := searchutils.KNNEuc(dp[j].Vec, generator, 1)
		// Search failed, put back into self.
		if len(indexes) == 0 {
			c.AddDataPoint(dp[j])
			continue
		}
		if err := receivers[indexes[0]].AddDataPoint(dp[j]); err != nil {
			// Adder failed, put back into self.
			c.AddDataPoint(dp[j])
		}
	}
}

func (c *Centroid) KNNDataPointLookupCos(vec []float64, k int, drain bool) []DataPoint {
	res := make([]DataPoint, 0, k)

	indexes := searchutils.KNNCos(vec, c.datapointVecGenerator(), k)
	for _, i := range indexes {
		res = append(res, c.DataPoints[i])
	}
	if drain {
		for _, i := range indexes {
			c.DataPoints = append(c.DataPoints[:i], c.DataPoints[i+1:]...)
		}
	}
	return res
}
