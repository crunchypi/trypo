package centoidmanager

import (
	"encoding/json"
	"testing"
	"time"
	"trypo/pkg/kmeans/centroid"
	"trypo/pkg/kmeans/common"
	"trypo/pkg/searchutils"
)

type datapoint struct {
	vec           []float64
	payload       []byte
	expires       time.Time
	expireEnabled bool
}

func (dp *datapoint) Vec() []float64 { return dp.vec }

func (dp *datapoint) Payload() []byte { return dp.payload }

func (dp *datapoint) Expired() bool {
	return dp.expireEnabled && time.Now().After(dp.expires)
}

// Tweak how long a 'time unit' is (used for timeouts). It
// standardises sleep time for these tests.
var _SLEEPUNIT = time.Millisecond * 10

// Helper for creating a vector, a lot nicer to write vec(1,2,3)
// instead of []float64{1,2,3}.
func vec(v ...float64) []float64 {
	_vec := make([]float64, len(v))
	for i, x := range v {
		_vec[i] = x
	}
	return _vec
}

// helper for creating a data point.
func dp(v []float64, sleepUnits int) *datapoint {
	_dp := datapoint{vec: v}

	if sleepUnits > 0 {
		_dp.expires = time.Now().Add(_SLEEPUNIT * time.Duration(sleepUnits))
		_dp.expireEnabled = true
	}
	return &_dp
}

func newCentroid(vec []float64) common.Centroid {
	args := centroid.NewCentroidArgs{
		InitVec:       vec,
		InitCap:       10,
		KNNSearchFunc: searchutils.KNNCos,
		KFNSearchFunc: searchutils.KFNCos,
	}
	centroid, ok := centroid.NewCentroid(args)
	if !ok {
		panic("couldn't setup Centroid")
	}
	return centroid
}

func newCentroidManager(vec []float64) *CentroidManager {
	cm, ok := NewCentroidManager(NewCentroidManagerArgs{
		InitVec:             vec,
		InitCap:             0,
		CentroidFactoryFunc: newCentroid,
		CentroidDPThreshold: 10,
		KNNSearchFunc:       searchutils.KNNCos,
		KFNSearchFunc:       searchutils.KFNCos,
	})
	if !ok {
		panic("couldn't setup CentroidManager for test")
	}
	return cm
}

func sleep() {
	time.Sleep(_SLEEPUNIT)
}

func TestCentroidDataPointPortions(t *testing.T) {
	cm := newCentroidManager(vec(0))

	c1 := newCentroid(vec(1))
	c2 := newCentroid(vec(1))
	c3 := newCentroid(vec(1))

	c1.AddDataPoint(dp(vec(1), 0))
	c1.AddDataPoint(dp(vec(1), 0))
	c2.AddDataPoint(dp(vec(1), 0))

	cm.Centroids = []common.Centroid{c1, c2, c3}
	//	map should be:
	//	{
	//		0:2,				It's c1.
	//		1:1,				It's c2.
	//		2:0,				It's c3.
	//	}
	//
	m := cm.centroidDataPointPortions(4)
	if m[0] != 2 || m[1] != 1 || m[2] != 0 {
		// JSON for pretty print.
		b, _ := json.MarshalIndent(m, "", "  ")
		t.Fatalf("incorrect map results:\n%s\n", b)
	}
}

func TestCentroidVecGenerator(t *testing.T) {
	cm := newCentroidManager(vec(0))
	c1 := newCentroid(vec(1))
	c2 := newCentroid(vec(2))
	cm.Centroids = []common.Centroid{c1, c2}

	gen := cm.centroidVecGenerator()

	vec, cont := gen()
	if vec[0] != 1 {
		t.Fatalf("incorrect first vector: %v", vec)
	}

	vec, cont = gen()
	if vec[0] != 2 {
		t.Fatalf("incorrect first vector: %v", vec)
	}

	_, cont = gen()
	if cont {
		t.Fatalf("generator didn't signal stop")
	}
}

func TestSplitCentroid(t *testing.T) {
	cm := newCentroidManager(vec(1))
	c1 := newCentroid(vec(1))
	cm.Centroids = []common.Centroid{c1}

	dps := []common.DataPoint{
		dp(vec(0), 0),
		dp(vec(1), 0),
		dp(vec(2), 0),
		dp(vec(3), 0),
	}

	for _, dp := range dps {
		c1.AddDataPoint(dp)
	}

	c2, splitOK := cm.splitCentroid(0, 2)
	if !splitOK {
		t.Fatalf("didn't split")
	}

	if dps := c1.DrainUnordered(99); len(dps) != 2 {
		t.Fatalf("unexpected c1dps len: %v", len(dps))
	}
	if dps := c2.DrainUnordered(99); len(dps) != 2 {
		t.Fatalf("unexpected c2dps len: %v", len(dps))
	}
}

func TestAddDataPoint(t *testing.T) {
	dps := []common.DataPoint{
		dp(vec(1, 1), 0),
		dp(vec(2, 2), 0),
	}
	// Case 1: No centroids in KMeans instance.
	cm := newCentroidManager(vec(0, 0))
	for _, dp := range dps {
		cm.AddDataPoint(dp)
	}
	if len(cm.Centroids) != 1 {
		t.Fatal("no new centroid")
	}
	if cm.Centroids[0].LenDP() != 2 {
		t.Fatal("new centroid didn't get enough datapoints")
	}

	// Case 2: 2 Centroids, each should get a datapoint each
	// due to their vector relationship to dps.
	cm = newCentroidManager(vec(0, 0))
	cm.Centroids = []common.Centroid{
		newCentroid(dps[0].Vec()),
		newCentroid(dps[1].Vec()),
	}

	for _, dp := range dps {
		cm.AddDataPoint(dp)
	}

	for i, c := range cm.Centroids {
		if c.LenDP() != 1 {
			t.Fatalf("centroid index %d doesn't have enough datapoints\n", i)
		}
		drain := c.DrainUnordered(1)
		if len(drain) == 0 {
			t.Fatal("Kmeans.Centroids[x].Drain(...) func impl error")
		}
		if drain[0].Vec()[0] != c.Vec()[0] {
			t.Fatalf("centroid index %d got incorrect dp: %v\n", i, c)
		}
	}

	// Case 3: Auto-splitting centroids.
	cm, ok := NewCentroidManager(NewCentroidManagerArgs{
		InitVec:             vec(0, 0),
		InitCap:             0,
		CentroidFactoryFunc: newCentroid,
		CentroidDPThreshold: 2,
		KNNSearchFunc:       searchutils.KNNCos,
		KFNSearchFunc:       searchutils.KFNCos,
	})
	if !ok {
		panic("couldn't setup centroid for test")
	}
	for _, dp := range dps {
		cm.AddDataPoint(dp)
	}
	if len(cm.Centroids) != 2 {
		t.Fatal("km obj didn't autosplit centroids. centroid len", len(cm.Centroids))
	}
}

func TestDrainUnordered(t *testing.T) {
	cm := newCentroidManager(vec(0, 0))

	c1 := newCentroid(vec(1))
	c2 := newCentroid(vec(1))

	c1.AddDataPoint(dp(vec(1), 0))
	c1.AddDataPoint(dp(vec(1), 0))
	c2.AddDataPoint(dp(vec(1), 0))

	cm.Centroids = []common.Centroid{c1, c2}

	dps := cm.DrainUnordered(2)
	if len(dps) != 2 {
		t.Fatal("incorrect drain amt:", len(dps))
	}
	if cm.Centroids[0].LenDP() != 1 {
		t.Fatal("remainder of dps in centroid 1 is incorrect:", c1.LenDP())
	}
	if cm.Centroids[1].LenDP() != 0 {
		t.Fatal("remainder of dps in centroid 2 is incorrect:", c2.LenDP())
	}
}

func TestDrainOrdered(t *testing.T) {
	cm := newCentroidManager(vec(0))

	c1 := newCentroid(vec(1, 1))
	c2 := newCentroid(vec(5, 5))

	c1.AddDataPoint(dp(vec(1, 2), 0)) // Should _not_ be drained.
	c1.AddDataPoint(dp(vec(1, 3), 0)) // Should be drained.
	c2.AddDataPoint(dp(vec(5, 5), 0)) // Should be drained as well.

	cm.Centroids = []common.Centroid{c1, c2}

	dps := cm.DrainOrdered(2)
	if len(dps) != 2 {
		t.Fatal("incorrect drain amt:", len(dps))
	}
	if cm.Centroids[0].LenDP() != 1 {
		t.Fatal("remainder of dps in centroid 1 is incorrect:", c1.LenDP())
	}
	if cm.Centroids[1].LenDP() != 0 {
		t.Fatal("remainder of dps in centroid 2 is incorrect:", c2.LenDP())
	}
	/*
		 The drain method tries to drain a uniform amount of datapoints from
		 each centroid, and that should be 1 each since:
				(1) Both centroids have at least 1 dp.
				(2) The km.DrainOrdered call above has 2 as input.

		But simply checking 'if dps[0].Vec[1] == 3' isn't enough
		because the map created in km.DrainOrdered is funnily not
		deterministic, even with a deterministic test...
		Hence the _and_ clause.
	*/
	if dps[0].Vec()[1] != 3 && dps[1].Vec()[1] != 3 {
		t.Fatal("didn't drain dp furthest from vec. dps:", dps)
	}
}
