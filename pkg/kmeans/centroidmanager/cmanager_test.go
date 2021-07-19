package centoidmanager

import (
	"encoding/json"
	"testing"
	"time"
	"trypo/pkg/kmeans/centroid"
	"trypo/pkg/kmeans/common"
	"trypo/pkg/searchutils"
)

/*
--------------------------------------------------------------------------------
NOTE: All these tests are using a cosine similarity func while setting up new
centroid managers (NewCentroidManagerArgs.KNNSearchFunc) AND concrete impl of
common.Centroid (see newCentroid + newCentroidManger in this file). So using
something else, like Euclidean distance will make all tests fail.

For sanity, this is checked and enforced in the init func.
--------------------------------------------------------------------------------
*/

var _knnSearchFunc = searchutils.KNNCos
var _kfnSearchFunc = searchutils.KFNCos

// See the note above. This init func validates the expected functionality
// of the dependency needed for tests in this file.
func init() {
	msg := "See note comment block right after import, someone broke the test!"

	// This init checks the correct _cosine_similarity_ functionality
	// of _knnSearchFunc and _kfnSearchFunc.

	vecs := [][]float64{
		{1, 5},
		{1, 8},
	}

	// Creates a generator that goes through the vecs above.
	createGen := func() func() ([]float64, bool) {
		i := 0
		return func() ([]float64, bool) {
			if i >= len(vecs) {
				return nil, false
			}
			i++
			return vecs[i-1], true
		}
	}
	// []float64{1,1} is closest to vecs[0]
	r := _knnSearchFunc([]float64{1, 1}, createGen(), 1)
	if r[0] != 0 {
		panic(msg)
	}
	// []float64{1,9} is closest to vecs[1]
	r = _knnSearchFunc([]float64{1, 9}, createGen(), 1)
	if r[0] != 1 {
		panic(msg)
	}

}

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

func vecEq(v1, v2 []float64) bool {
	if len(v1) != len(v2) {
		return false
	}
	for i := 0; i < len(v1); i++ {
		if v1[i] != v2[i] {
			return false
		}
	}
	return true
}

func dps2Vecs(dps []common.DataPoint) [][]float64 {
	res := make([][]float64, len(dps))
	for i, dp := range dps {
		res[i] = dp.Vec()
	}
	return res
}

func containsVec(vec []float64, vecs [][]float64) bool {
	for _, other := range vecs {
		if vecEq(vec, other) {
			return true
		}
	}
	return false
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
		KNNSearchFunc: _knnSearchFunc,
		KFNSearchFunc: _kfnSearchFunc,
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
		KNNSearchFunc:       _knnSearchFunc,
		KFNSearchFunc:       _kfnSearchFunc,
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

// AddDataPoint case 1: No Centroids in CentroidManager.
func TestAddDataPointFirst(t *testing.T) {
	cm := newCentroidManager(vec(0, 0))
	dps := []common.DataPoint{
		dp(vec(1, 1), 0),
		dp(vec(2, 2), 0),
	}
	for _, dp := range dps {
		cm.AddDataPoint(dp)
	}
	if len(cm.Centroids) != 1 {
		t.Fatal("no new centroid")
	}
	if cm.Centroids[0].LenDP() != 2 {
		t.Fatal("new centroid didn't get enough datapoints")
	}
}

// AddDataPoint case 2: 2 Centroids exist, each should get a dp.
func TestAddDataPointOldCentroids(t *testing.T) {
	dps := []common.DataPoint{
		dp(vec(1, 2), 0),
		dp(vec(1, 3), 0),
	}

	cm := newCentroidManager(vec(0, 0))
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
}

// AddDataPoint case 3: Auto-adjusting internal vector.
func TestAddDataPointInternalVec(t *testing.T) {
	d1 := dp(vec(1, 1), 0)
	c1 := newCentroid(d1.Vec())
	c1.AddDataPoint(d1)

	d2 := dp(vec(3, 3), 0)
	c2 := newCentroid(d2.Vec())

	// vec(2,2) is mean of d1.vec + d2.vec
	cm := newCentroidManager(vec(2, 2))
	cm.Centroids = []common.Centroid{c1, c2}

	// mean(d1 + d2 + vec(5,5)) = vec(3,3)
	cm.AddDataPoint(dp(vec(5, 5), 0))
	if !vecEq(cm.Vec(), vec(3, 3)) {
		t.Errorf("didn't auto-adjust cm vector correctly: %v", cm.Vec())
	}
}

// AddDataPoint case 4: Auto-splitting centroids.
func TestAddDataPointSplit(t *testing.T) {
	dps := []common.DataPoint{
		dp(vec(1, 1), 0),
		dp(vec(2, 2), 0),
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

	c1 := newCentroid(vec(0, 0))
	c2 := newCentroid(vec(0, 0))

	// This setup might look a bit weird, since c1 adds 3 dps
	// with the same vector first. This tests assumes that
	// kmeans/centroid.Centroid is used, and that impl (at the
	// moment of writing) updates the internal vector on each add.
	// vec(1,3) is added three times just to make sure that vec(1,9)
	// is definitively furthest away from the mean.
	c1.AddDataPoint(dp(vec(1, 3), 0)) // Should _not_ be drained.
	c1.AddDataPoint(dp(vec(1, 3), 0)) // Should _not_ be drained.
	c1.AddDataPoint(dp(vec(1, 3), 0)) // Should _not_ be drained.
	c1.AddDataPoint(dp(vec(1, 9), 0)) // Should be drained.
	c2.AddDataPoint(dp(vec(5, 5), 0)) // Should be drained as well.

	cm.Centroids = []common.Centroid{c1, c2}

	dps := cm.DrainOrdered(2)
	if len(dps) != 2 {
		t.Fatal("incorrect drain amt:", len(dps))
	}
	if cm.Centroids[0].LenDP() != 3 {
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

		But simply checking 'if dps[0].Vec[1] == 9' isn't enough
		because the map created in km.DrainOrdered is funnily not
		deterministic, even with a deterministic test...
		Hence the _and_ clause.
	*/
	if dps[0].Vec()[1] != 9 && dps[1].Vec()[1] != 9 {
		t.Fatal("didn't drain dp furthest from vec. dps:", dps)
	}
}

func TestExpire(t *testing.T) {
	c1 := newCentroid(vec(1))
	c2 := newCentroid(vec(1))

	c1.AddDataPoint(dp(vec(2), 1))
	c2.AddDataPoint(dp(vec(3), 0))

	cm := newCentroidManager(vec(1))
	cm.Centroids = []common.Centroid{c1, c2}

	sleep()
	cm.Expire()

	if cm.Centroids[0].LenDP() != 0 {
		t.Fatal("centroid 1 (c1) has an outdated datapoint that was not removed")
	}
	if cm.Centroids[1].LenDP() != 1 {
		t.Fatal("centroid 2 (c2) don't have an outdated datapoint but it was removed")
	}
}

func TestLen(t *testing.T) {}

func TestMemTrim(t *testing.T) {}

func TestMoveVector(t *testing.T) {
	dp1 := dp(vec(1, 1), 0)
	dp2 := dp(vec(3, 3), 0)
	dp3 := dp(vec(3, 3), 0)
	dp4 := dp(vec(5, 5), 0)

	c1 := newCentroid(vec(0, 0))
	c2 := newCentroid(vec(0, 0))

	for _, dp := range []common.DataPoint{dp1, dp2} {
		c1.AddDataPoint(dp)
	}
	for _, dp := range []common.DataPoint{dp3, dp4} {
		c2.AddDataPoint(dp)
	}

	cm := newCentroidManager(vec(0, 0))
	cm.Centroids = []common.Centroid{c1, c2}

	cm.MoveVector()

	// Mean of dp1&dp2 = {2,2}
	// Mean of dp3&dp4 = {4,4}
	// Mean of c1&c2 = {3,3}
	if cm.Centroids[0].Vec()[0] != 2 {
		t.Fatal("incorrect vec in c1: ", cm.Centroids[0].Vec())
	}
	if cm.Centroids[1].Vec()[0] != 4 {
		t.Fatal("incorrect vec in c2: ", cm.Centroids[0].Vec())
	}
	if cm.Vec()[0] != 3 {
		t.Fatal("incorrect vec in cm:", cm.Vec())
	}
}

func TestDistributeDataPoints(t *testing.T) {
	// The centroid and datapoint setup below is set up such that
	// dp4 is in c1 but is actually closer to c2. Likewise, dp8
	// is in c2 but is closer to c1.

	// These 2 vectors don't matter. This test assumed that
	// kmeans/centroid.Centroid is used, and that impl (at the
	// moment of writing) updates the internal vector or each dp
	// addition (which will be done in the next code blocks).
	c1 := newCentroid(vec(1, 1))
	c2 := newCentroid(vec(1, 1))

	// Mean: (1, 4.5) : Small angle.
	c1.AddDataPoint(dp(vec(1, 3), 0)) // dp1
	c1.AddDataPoint(dp(vec(1, 3), 0)) // dp2
	c1.AddDataPoint(dp(vec(1, 3), 0)) // dp3
	c1.AddDataPoint(dp(vec(1, 9), 0)) // dp4: closest to c2.

	// Mean: (1, 7.5) : Large angle
	c2.AddDataPoint(dp(vec(1, 9), 0)) // dp5
	c2.AddDataPoint(dp(vec(1, 9), 0)) // dp6
	c2.AddDataPoint(dp(vec(1, 9), 0)) // dp7
	c2.AddDataPoint(dp(vec(1, 3), 0)) // dp8: closest to c1.

	// case: one of the centroids is 'external' (i.e not in CentroidManager).
	// This is done twice for symmetry: (1) c1 is internal in cm while c2 is
	// external, and vice versa. This is so that all dps get to their centroid.

	// (1) c1 is internal, c2 is external.
	cm := newCentroidManager(vec(0))
	cm.Centroids = []common.Centroid{c1}
	cm.DistributeDataPoints(1, []common.DataPointReceiver{c2})
	c1 = cm.Centroids[0] // Save before new cm.

	// (2) c2 is internal, c1 is external.
	cm = newCentroidManager(vec(0))
	cm.Centroids = []common.Centroid{c2}
	cm.DistributeDataPoints(1, []common.DataPointReceiver{c1})
	c2 = cm.Centroids[0] // Save again for easy readings.

	c1dps := c1.DrainUnordered(9) // Convenience
	c2dps := c2.DrainUnordered(9) // Convenience

	if len(c1dps) != 4 {
		t.Fatalf("incorrect dp amount in c1: %v\n", len(c1dps))
	}
	if len(c2dps) != 4 {
		t.Fatalf("incorrect dp amount in c2: %v\n", len(c2dps))
	}
	// Confirm that dp4 is no longer in c1 (moved to c2).
	if containsVec(vec(1, 9), dps2Vecs(c1dps)) {
		t.Fatalf("c1dps still contains vec with bad fit.")
	}

	// Confirm that dp8 is no longer in c2 (moved to c1).
	if containsVec(vec(1, 3), dps2Vecs(c2dps)) {
		t.Fatalf("c1dps still contains vec with bad fit.")
	}
}

// Very similar to TestDistributeDataPoints; that test handles centroids
// that can be external to a CentroidManager. Another functionality of
// CentroidManager.DistributeDataPoints is that it acts differently when
// it gets a nil for the receivers arg -- then, all datapoints are
// distributed internally to their best centroid.
// Note, setup (creating centroids + inserting datapoints) and validation
// (all the if statements) are exactly the same.
func TestDistributeDataPointsNil(t *testing.T) {
	// The centroid and datapoint setup below is set up such that
	// dp4 is in c1 but is actually closer to c2. Likewise, dp8
	// is in c2 but is closer to c1.

	// These 2 vectors don't matter. This test assumed that
	// kmeans/centroid.Centroid is used, and that impl (at the
	// moment of writing) updates the internal vector or each dp
	// addition (which will be done in the next code blocks).
	c1 := newCentroid(vec(1, 1))
	c2 := newCentroid(vec(1, 1))

	// Mean: (1, 4.5) : Small angle.
	c1.AddDataPoint(dp(vec(1, 3), 0)) // dp1
	c1.AddDataPoint(dp(vec(1, 3), 0)) // dp2
	c1.AddDataPoint(dp(vec(1, 3), 0)) // dp3
	c1.AddDataPoint(dp(vec(1, 9), 0)) // dp4: closest to c2.

	// Mean: (1, 7.5) : Large angle
	c2.AddDataPoint(dp(vec(1, 9), 0)) // dp5
	c2.AddDataPoint(dp(vec(1, 9), 0)) // dp6
	c2.AddDataPoint(dp(vec(1, 9), 0)) // dp7
	c2.AddDataPoint(dp(vec(1, 3), 0)) // dp8: closest to c1.

	cm := newCentroidManager(vec(0))
	cm.Centroids = []common.Centroid{c1, c2}

	// This should move dps as specified above.
	cm.DistributeDataPoints(2, nil)

	c1dps := c1.DrainUnordered(9) // Convenience
	c2dps := c2.DrainUnordered(9) // Convenience

	if len(c1dps) != 4 {
		t.Fatalf("incorrect dp amount in c1: %v\n", len(c1dps))
	}
	if len(c2dps) != 4 {
		t.Fatalf("incorrect dp amount in c2: %v\n", len(c2dps))
	}
	// Confirm that dp4 is no longer in c1 (moved to c2).
	if containsVec(vec(1, 9), dps2Vecs(c1dps)) {
		t.Fatalf("c1dps still contains vec with bad fit.")
	}

	// Confirm that dp8 is no longer in c2 (moved to c1).
	if containsVec(vec(1, 3), dps2Vecs(c2dps)) {
		t.Fatalf("c1dps still contains vec with bad fit.")
	}
}

func TestNearestCentroid(t *testing.T) {
	c1 := newCentroid(vec(1, 2))
	c2 := newCentroid(vec(1, 3))
	c3 := newCentroid(vec(1, 4))

	cm := newCentroidManager(vec(0, 0))
	cm.Centroids = []common.Centroid{c1, c2, c3}

	c, ok := cm.NearestCentroid(vec(1, 5))
	if !ok {
		t.Fatal("didn't get any centroid")
	}
	if c.Vec()[1] != c3.Vec()[1] {
		t.Fatalf("incorrect centroid with vec %v", c.Vec())
	}
}

func TestSplit(t *testing.T) {
	dps := []common.DataPoint{
		dp(vec(1), 0),
		dp(vec(1), 0),
		dp(vec(1), 0),
		dp(vec(1), 0),
	}
	cm := newCentroidManager(vec(0))
	cm.centroidDPThreshold = len(dps) + 1
	for _, dp := range dps {
		cm.AddDataPoint(dp)
	}

	cm.SplitCentroids(func(c common.Centroid) bool {
		return c.LenDP() > 2
	})
	if len(cm.Centroids) != 2 {
		t.Fatal("incorrect centroid count after split")
	}

	l1 := cm.Centroids[0].LenDP()
	l2 := cm.Centroids[1].LenDP()
	if l1 != 2 || l2 != 2 {
		t.Fatal("uneven datapoint distribution after split:", l1, l2)
	}
}

func TestMergeCentroids(t *testing.T) {
	c1 := newCentroid(vec(1, 1))
	c2 := newCentroid(vec(1, 9))
	c3 := newCentroid(vec(1, 2)) // closest to c1.

	// Vecs here do not matter.
	c1.AddDataPoint(dp(vec(0, 0), 0))
	c2.AddDataPoint(dp(vec(0, 0), 0))
	c3.AddDataPoint(dp(vec(0, 0), 0))
	c3.AddDataPoint(dp(vec(0, 0), 0))

	cm := newCentroidManager(vec(0, 0))
	cm.Centroids = []common.Centroid{c1, c2, c3}

	cm.MergeCentroids(func(c common.Centroid) bool {
		// Merge condition for c3. So the nearest, c1, should
		// be merged into it (c3).
		return c.LenDP() == 2
	})
	if len(cm.Centroids) != 2 {
		t.Fatalf("unexpected cm.Centroids len: %v", len(cm.Centroids))
	}
	// 1) Merge cond for c3, nearest is c1.
	// 2) c1 merged into c3.
	// 3) cm.Centroids= [c2, c3].
	if cm.Centroids[1].LenDP() != 3 {
		t.Fatalf("c3 didn't get merged into c1")
	}

}
