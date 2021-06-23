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
