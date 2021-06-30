package kmeans

import (
	"encoding/json"
	"testing"
	"time"
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

func TestKMDrainUnordered(t *testing.T) {
	c1 := NewCentroidFromVec([]float64{1})
	c2 := NewCentroidFromVec([]float64{1})

	c1.AddDataPoint(DataPoint{Vec: []float64{1}})
	c1.AddDataPoint(DataPoint{Vec: []float64{1}})
	c2.AddDataPoint(DataPoint{Vec: []float64{1}})

	km := KMeans{Centroids: newCentroidSlice(2, 2)}
	for i, c := range []*Centroid{c1, c2} {
		km.Centroids[i] = c
	}
	dps := km.DrainUnordered(2)
	if len(dps) != 2 {
		t.Fatal("incorrect drain amt:", len(dps))
	}
	if km.Centroids[0].LenDP() != 1 {
		t.Fatal("remainder of dps in centroid 1 is incorrect:", len(c1.DataPoints))
	}
	if km.Centroids[1].LenDP() != 0 {
		t.Fatal("remainder of dps in centroid 2 is incorrect:", len(c2.DataPoints))
	}
}

func TestKMDrainOrdered(t *testing.T) {
	c1 := NewCentroidFromVec([]float64{1})
	c2 := NewCentroidFromVec([]float64{5})

	c1.AddDataPoint(DataPoint{Vec: []float64{2}}) // 1 away from c1.vec
	c1.AddDataPoint(DataPoint{Vec: []float64{3}}) // 2 away from c1.vec
	c2.AddDataPoint(DataPoint{Vec: []float64{6}}) // 1 away from c2.vec

	km := KMeans{Centroids: newCentroidSlice(2, 2)}
	for i, c := range []*Centroid{c1, c2} {
		km.Centroids[i] = c
	}

	dps := km.DrainOrdered(2)
	if len(dps) != 2 {
		t.Fatal("incorrect drain amt:", len(dps))
	}
	if km.Centroids[0].LenDP() != 1 {
		t.Fatal("remainder of dps in centroid 1 is incorrect:", len(c1.DataPoints))
	}
	if km.Centroids[1].LenDP() != 0 {
		t.Fatal("remainder of dps in centroid 2 is incorrect:", len(c2.DataPoints))
	}
	/*
		 The drain method tries to drain a uniform amount of datapoints from
		 each centroid, and that should be 1 each since:
				(1) Both centroids have at least 1 dp.
				(2) The km.DrainOrdered call above has 2 as input.

		But simply checking 'if dps[0].Vec[0] == 3' isn't enough
		because the map created in km.DrainOrdered is funnily not
		deterministic (found out through testing...). Hence the
		_and_ clause.
	*/
	if dps[0].Vec[0] != 3 && dps[1].Vec[0] != 3 {
		t.Fatal("didn't drain dp furthest from vec. dps:", dps)
	}
}

func TestKMExpireDataPoints(t *testing.T) {
	c1 := NewCentroidFromVec([]float64{1})
	c2 := NewCentroidFromVec([]float64{1})

	c1.AddDataPoint(DataPoint{Vec: []float64{2}, Expire: time.Now(), ExpireEnabled: true})
	c2.AddDataPoint(DataPoint{Vec: []float64{3}})

	km := KMeans{Centroids: newCentroidSlice(2, 2)}
	for i, c := range []*Centroid{c1, c2} {
		km.Centroids[i] = c
	}

	km.ExpireDataPoints()

	if km.Centroids[0].LenDP() != 0 {
		t.Fatal("centroid 1 (c1) has an outdated datapoint that was not removed")
	}
	if km.Centroids[1].LenDP() != 1 {
		t.Fatal("centroid 2 (c2) don't have an outdated datapoint but it was removed")
	}
}

func TestKMLen(t *testing.T) {}

func TestKMMemTrim(t *testing.T) {}

func TestKMMoveVector(t *testing.T) {

	dp1 := DataPoint{Vec: []float64{1, 1}}
	dp2 := DataPoint{Vec: []float64{3, 3}}
	dp3 := DataPoint{Vec: []float64{3, 3}}
	dp4 := DataPoint{Vec: []float64{5, 5}}

	c1 := NewCentroidFromVec([]float64{0, 0})
	c2 := NewCentroidFromVec([]float64{0, 0})

	for _, dp := range []DataPoint{dp1, dp2} {
		c1.AddDataPoint(dp)
	}
	for _, dp := range []DataPoint{dp3, dp4} {
		c2.AddDataPoint(dp)
	}

	km := KMeans{Centroids: newCentroidSlice(2, 2)}
	for i, c := range []*Centroid{c1, c2} {
		km.Centroids[i] = c
	}

	km.MoveVector()

	// Mean of dp1&dp2 = {2,2}
	// Mean of dp3&dp4 = {4,4}
	// Mean of c1&c2 = {3,3}
	if km.Centroids[0].Vec()[0] != 2 {
		t.Fatal("incorrect vec in c1: ", km.Centroids[0].Vec())
	}
	if km.Centroids[1].Vec()[0] != 4 {
		t.Fatal("incorrect vec in c2: ", km.Centroids[0].Vec())
	}
	if km.Vec()[0] != 3 {
		t.Fatal("incorrect vec in km:", km.Vec())
	}
}

func TestKMSplit(t *testing.T) {
	dps := []DataPoint{
		{Vec: []float64{1}},
		{Vec: []float64{1}},
		{Vec: []float64{1}},
		{Vec: []float64{1}},
	}
	km := KMeans{CentroidDPThreshold: 10}
	for _, dp := range dps {
		km.AddDataPoint(dp)
	}
	km.Split(2)
	if len(km.Centroids) != 2 {
		t.Fatal("incorrect centroid count after split")
	}

	l1 := km.Centroids[0].LenDP()
	l2 := km.Centroids[1].LenDP()
	if l1 != 2 || l2 != 2 {
		t.Fatal("uneven datapoint distribution after split:", l1, l2)
	}
}

func TestKMDistributeDataPoints(t *testing.T) {
	/*
		KMeans.DistributeDataPoints handles both nil and non-nil arguments very
		differently, so this test will be in 2 phases, one for each case.

	*/

	setup := func() (*Centroid, *Centroid) {
		/*
			The centroid and datapoint setup below is set up such that
			dp2 is in c1 but is actually closer to c2. Likewise, dp3
			is in c2 but is closer to c1.
		*/
		c1 := NewCentroidFromVec([]float64{1})
		c2 := NewCentroidFromVec([]float64{5})

		dp1 := DataPoint{Vec: []float64{2}} // closest to c1.vec
		dp2 := DataPoint{Vec: []float64{6}} // closest to c2.vec
		for _, dp := range []DataPoint{dp1, dp2} {
			c1.AddDataPoint(dp)
		}

		dp3 := DataPoint{Vec: []float64{2}} // closest to c1.vec
		dp4 := DataPoint{Vec: []float64{6}} // closest to c2.vec
		for _, dp := range []DataPoint{dp3, dp4} {
			c2.AddDataPoint(dp)
		}
		return c1, c2
	}

	test := func(no int, c1, c2 *Centroid) {
		/*
			Confirms that the c1 has dp1 and dp3, while c2 has
			dp2 and dp4, as set up in the setup func above.
		*/
		c1dps := c1.DataPoints // Convenience
		c2dps := c2.DataPoints // Convenience

		if len(c1dps) != 2 {
			t.Fatalf("(case %d) incorrect dp amount in c1: %v\n", no, len(c1dps))
		}
		if len(c2dps) != 2 {
			t.Fatalf("(case %d) incorrect dp amount in c1: %v\n", no, len(c2dps))
		}
		if c1dps[0].Vec[0] != 2 || c1dps[1].Vec[0] != 2 {
			t.Fatalf("(case %d) incorrect dp placement in c1: %v\n", no, c1dps)
		}
		if c2dps[0].Vec[0] != 6 || c2dps[1].Vec[0] != 6 {
			t.Fatalf("(case %d) incorrect dp placement in c1: %v\n", no, c2dps)
		}

	}

	// First case: nil (will distribute DPs between contained Centroids)
	c1, c2 := setup()
	km := KMeans{Centroids: newCentroidSlice(2, 2)}
	for i, c := range []*Centroid{c1, c2} {
		km.Centroids[i] = c
	}

	km.DistributeDataPoints(2, nil)
	test(1, km.Centroids[0].(*Centroid), km.Centroids[1].(*Centroid))

	/*
		 Second case: one of the centroids are 'external' (i.e not in Kmeans).
		 This has to be done twice: (1) c1 is internal in km while c2 is external,
		 and vice versa.
		c1, c2 = setup()
	*/
	km = KMeans{Centroids: newCentroidSlice(1, 1)}
	km.Centroids[0] = c1

	km.DistributeDataPoints(1, []interface {
		VecContainer
		DataPointAdder
	}{c2})
	c1 = km.Centroids[0].(*Centroid) // Save the internal, this should have 3 datapoints now.

	km = KMeans{Centroids: newCentroidSlice(1, 1)}
	km.Centroids[0] = c2

	km.DistributeDataPoints(1, []interface {
		VecContainer
		DataPointAdder
	}{c1})

	c2 = km.Centroids[0].(*Centroid) // Save again for easier access in the test(...) call.
	// Both c1 and c2 should now have 2 datapoints each.
	test(2, c1, c2)
}
