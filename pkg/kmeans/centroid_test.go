package kmeans

import (
	"testing"
	"time"
)

func TestCAddDataPoint(t *testing.T) {
	c := Centroid{vec: []float64{1, 1, 1}}
	c.AddDataPoint(DataPoint{Vec: []float64{1, 1, 1}})
	if len(c.DataPoints) != 1 {
		t.Error("didn't add datapoint")
	}
}

func TestCDataPointVecGenerator(t *testing.T) {
	c := Centroid{
		vec: []float64{1, 1, 1},
		DataPoints: []DataPoint{
			{Vec: []float64{1, 1, 1}},
			{Vec: []float64{2, 2, 2}, Expire: time.Now(), ExpireEnabled: true},
		},
	}
	gen := c.datapointVecGenerator()
	vec, _ := gen()
	if vec[0] != 1 {
		t.Fatalf("generator produced incorrect res: %v", vec)
	}
	_, cont := gen()
	if cont {
		t.Fatalf("second generator call signals continue")
	}
	if len(c.DataPoints) != 1 {
		t.Fatalf("generator didn't expire one datapoint")
	}
	if c.DataPoints[0].Vec[0] != 1 {
		t.Fatalf("generator expired incorrect datapoint")
	}
}

func TestCDrainUnordered(t *testing.T) {
	c := Centroid{
		vec: []float64{1, 1},
		DataPoints: []DataPoint{
			// Should be dropped since it expires.
			{Vec: []float64{2, 2}, Expire: time.Now(), ExpireEnabled: true},
			// Should be returned.
			{Vec: []float64{3, 3}},
			// Should be left alone if arg to c.DrainUnordered is 1.
			{Vec: []float64{4, 4}},
		},
	}
	dps := c.DrainUnordered(1)
	if len(c.DataPoints) != 1 {
		t.Fatalf("drain issue, incorrect amt of dps in centroid: %v", c.DataPoints)
	}
	if c.DataPoints[0].Vec[0] != 4 {
		t.Fatalf("drain issue, incorrect dp remainder in centroid: %v", c.DataPoints)
	}
	if len(dps) != 1 {
		t.Fatalf("drain issue, result should be of len 1: %v", dps)
	}
	if dps[0].Vec[0] != 3 {
		t.Fatalf("drain issue: inorrect drain result: %v", dps)
	}
}

func TestCDrainOrdered(t *testing.T) {
	// @ Flawed/incomplete case. DPs with vecs [1,1] and [2,2], with c.Vec [0,0]
	// @ have caused an issue earlier (before a rewrite of the current DrainOrdered func).
	c := Centroid{
		vec: []float64{1, 1},
		DataPoints: []DataPoint{
			// Should be dropped since it expires.
			{Vec: []float64{2, 2}, Expire: time.Now(), ExpireEnabled: true},
			// Should be left alone (if arg to c.DrainOrdered is 1), as it is closer
			// to the centroid (euclidean dist), compared to the next DataPoint.
			{Vec: []float64{3, 3}},
			// Should be returned since it's furthest away in euclidean space to the centroid.
			{Vec: []float64{4, 4}},
		},
	}
	dps := c.DrainOrdered(1)
	if len(c.DataPoints) != 1 {
		t.Fatalf("drain issue, incorrect amt of dps in centroid: %v", c.DataPoints)
	}
	if c.DataPoints[0].Vec[0] != 3 {
		t.Fatalf("drain issue, incorrect dp remainder in centroid: %v", c.DataPoints)
	}
	if len(dps) != 1 {
		t.Fatalf("drain issue, result should be of len 1: %v", dps)
	}
	if dps[0].Vec[0] != 4 {
		t.Fatalf("drain issue: inorrect drain result: %v", dps)
	}
}
