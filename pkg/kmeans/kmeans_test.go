package kmeans

import (
	"encoding/json"
	"testing"
)

func TestKMAddDataPoint(t *testing.T) {
	dps := []DataPoint{
		{Vec: []float64{1, 1}},
		{Vec: []float64{2, 2}},
	}
	// Case 1: No centroids in KMeans instance.
	km := KMeans{}
	for _, dp := range dps {
		km.AddDataPoint(dp)
	}
	if len(km.Centroids) != 1 {
		t.Fatal("no new centroid")
	}
	if km.Centroids[0].LenDP() != 2 {
		t.Fatal("new centroid didn't get enough datapoints")
	}

	// Case 2: 2 Centroids, each should get a datapoint each
	// due to their vector relationship to dps.
	km = KMeans{Centroids: newCentroidSlice(2, 2)}
	km.Centroids[0] = NewCentroidFromVec(dps[0].Vec)
	km.Centroids[1] = NewCentroidFromVec(dps[1].Vec)

	for _, dp := range dps {
		km.AddDataPoint(dp)
	}

	for i, c := range km.Centroids {
		if c.LenDP() != 1 {
			t.Fatalf("centroid index %d doesn't have enough datapoints\n", i)
		}
		drain := c.DrainUnordered(1)
		if len(drain) == 0 {
			t.Fatal("Kmeans.Centroids[x].Drain(...) func impl error")
		}
		if drain[0].Vec[0] != c.Vec()[0] {
			t.Fatalf("centroid index %d got incorrect dp: %v\n", i, c)
		}
	}

	// Case 3: Auto-splitting centroids.
	km = KMeans{CentroidDPThreshold: 1}
	for _, dp := range dps {
		km.AddDataPoint(dp)
	}
	if len(km.Centroids) != 2 {
		t.Fatal("km obj didn't autosplit centroids. centroid len", len(km.Centroids))
	}

}

func TestKMCentroidDataPointProportions(t *testing.T) {
	c1 := NewCentroidFromVec([]float64{1})
	c2 := NewCentroidFromVec([]float64{1})
	c3 := NewCentroidFromVec([]float64{1})

	c1.AddDataPoint(DataPoint{Vec: []float64{1}})
	c1.AddDataPoint(DataPoint{Vec: []float64{1}})
	c2.AddDataPoint(DataPoint{Vec: []float64{1}})

	km := KMeans{Centroids: newCentroidSlice(3, 3)}
	for i, c := range []*Centroid{c1, c2, c3} {
		km.Centroids[i] = c
	}
	/*
		map should be:
			{
				0:2,
				1:1,
				2:0,
			}
	*/
	m := km.centroidDataPointPortions(4)
	if m[0] != 2 || m[1] != 1 || m[2] != 0 {
		// JSON for pretty print.
		b, _ := json.MarshalIndent(m, "", "  ")
		t.Fatalf("incorrect map results:\n%s\n", b)
	}

}
