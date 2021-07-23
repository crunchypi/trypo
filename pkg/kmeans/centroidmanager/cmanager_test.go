package centroidmanager

import (
	"encoding/json"
	"testing"
	"time"
	"trypo/pkg/kmeans/centroid"
	"trypo/pkg/kmeans/common"
	"trypo/pkg/mathutils"
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

/*
--------------------------------------------------------------------------------
Section for utils.
--------------------------------------------------------------------------------
*/

// Tweak how long a 'time unit' is (used for timeouts). It
// standardises sleep time for these tests.
var _SLEEPUNIT = time.Millisecond * 10

// Vec tools aliases.
var vec = mathutils.Vec     // Create new vec.
var vecEq = mathutils.VecEq // compare two vecs.
var vecIn = mathutils.VecIn // Check if []vec contains vec.

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

// helper for creating a data point.
func dp(v []float64, sleepUnits int) *datapoint {
	_dp := datapoint{vec: v}

	if sleepUnits > 0 {
		_dp.expires = time.Now().Add(_SLEEPUNIT * time.Duration(sleepUnits))
		_dp.expireEnabled = true
	}
	return &_dp
}

func dps2Vecs(dps []common.DataPoint) [][]float64 {
	res := make([][]float64, len(dps))
	for i, dp := range dps {
		res[i] = dp.Vec()
	}
	return res
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

/*
--------------------------------------------------------------------------------
Proper test section.
--------------------------------------------------------------------------------
*/

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

	v, cont := gen()
	if !vecEq(v, c1.Vec()) {
		t.Fatalf("incorrect first vector: %v", v)
	}

	v, cont = gen()
	if !vecEq(v, c2.Vec()) {
		t.Fatalf("incorrect first vector: %v", v)
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
	if !t.Run("Test Dependency 1", TestMoveVector) {
		t.Fatalf("Expected TestMoveVector to work, it did not.")
	}
	// Note, all vecs below are arbitrary.

	c1 := newCentroid(vec(1, 1))
	c2 := newCentroid(vec(3, 2))

	cm := newCentroidManager(vec(2, 2))
	cm.Centroids = []common.Centroid{c1, c2}
	cm.MoveVector()

	cm.AddDataPoint(dp(vec(5, 5), 0))

	vecBkp := vec(cm.vec...)
	cm.MoveVector()
	if !vecEq(vecBkp, cm.vec) {
		t.Fatalf("auto-adjusted cm vec is incorrect. want %v, have %v", cm.vec, vecBkp)
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
	if !t.Run("Test Dependency 1", TestMoveVector) {
		t.Fatalf("Expected TestMoveVector to work, it did not.")
	}
	cm := newCentroidManager(vec(0))

	c1 := newCentroid(vec(0))
	c2 := newCentroid(vec(0))

	c1.AddDataPoint(dp(vec(1), 0)) // dp1.
	c1.AddDataPoint(dp(vec(3), 0)) // dp2.
	c2.AddDataPoint(dp(vec(5), 0)) // dp3.

	cm.Centroids = []common.Centroid{c1, c2}
	cm.MoveVector() // For auto-adjusting vec test.

	// dp 1 & 3 should be removed, as dp1 is first in c1
	// and dp2 is first in c2.
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

	// Auto-adjust vec test.
	vecBkp := vec(cm.vec...)
	cm.MoveVector()
	if !vecEq(vecBkp, cm.vec) {
		t.Fatalf("auto-adjusted cm vec is incorrect. want %v, have %v", cm.vec, vecBkp)
	}
}

func TestDrainOrdered(t *testing.T) {
	if !t.Run("Test Dependency 1", TestMoveVector) {
		t.Fatalf("Expected TestMoveVector to work, it did not.")
	}
	cm := newCentroidManager(vec(1, 1))

	c1 := newCentroid(vec(1, 1))
	c2 := newCentroid(vec(1, 1))

	// This setup might look a bit weird, since c1 adds 3 dps
	// with the same vector first. This tests assumes that
	// kmeans/centroid.Centroid is used, and that impl (at the
	// moment of writing) updates the internal vector on each add.
	// vec(1,3) is added three times just to make sure that vec(1,9)
	// is definitively furthest away from the mean.
	c1.AddDataPoint(dp(vec(1, 3), 0)) // dp1, Should _not_ be drained.
	c1.AddDataPoint(dp(vec(1, 3), 0)) // dp2, Should _not_ be drained.
	c1.AddDataPoint(dp(vec(1, 3), 0)) // dp3, Should _not_ be drained.
	c1.AddDataPoint(dp(vec(1, 9), 0)) // dp4, Should be drained.
	c2.AddDataPoint(dp(vec(5, 5), 0)) // dp5, Should be drained as well.

	cm.Centroids = []common.Centroid{c1, c2}
	cm.MoveVector() // For auto-adjusting vec test.

	// The drain method tries to drain a uniform amount of dps from
	// each centroid, and that should be 1 each since:
	//	(1) Both centroids have at least 1 dp.
	//	(2) The km.DrainOrdered call below has 2 as input.
	// So dp5 should be drained, as it is the only dp in c2, while
	// dp4 should be drained from c1, given that it is 'furthest'
	// away from the c1 vec (furthest in a cosine similarity sense).
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
	if !vecIn(vec(1, 9), dps2Vecs(dps)) {
		t.Fatal("didn't drain dp4 from c1:")
	}
	if !vecIn(vec(5, 5), dps2Vecs(dps)) {
		t.Fatal("didn't drain dp5 from c2:")
	}

	// Auto-adjust vec test.
	vecBkp := vec(cm.vec...)
	cm.MoveVector()
	if !vecEq(vecBkp, cm.vec) {
		t.Fatalf("auto-adjusted cm vec is incorrect. want %v, have %v", cm.vec, vecBkp)
	}

}

func TestExpire(t *testing.T) {
	if !t.Run("Test Dependency 1", TestMoveVector) {
		t.Fatalf("Expected TestMoveVector to work, it did not.")
	}

	c1 := newCentroid(vec(1))
	c2 := newCentroid(vec(1))

	c1.AddDataPoint(dp(vec(2), 1))
	c2.AddDataPoint(dp(vec(3), 0))

	cm := newCentroidManager(vec(1))
	cm.Centroids = []common.Centroid{c1, c2}
	cm.MoveVector() // For auto-adjusting vec test.

	sleep()
	cm.Expire()

	if cm.Centroids[0].LenDP() != 0 {
		t.Fatal("centroid 1 (c1) has an outdated datapoint that was not removed")
	}
	if cm.Centroids[1].LenDP() != 1 {
		t.Fatal("centroid 2 (c2) don't have an outdated datapoint but it was removed")
	}

	// Auto-adjust vec test.
	vecBkp := vec(cm.vec...)
	cm.MoveVector()
	if !vecEq(vecBkp, cm.vec) {
		t.Fatalf("auto-adjusted cm vec is incorrect. want %v, have %v", cm.vec, vecBkp)
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
	if !vecEq(cm.Centroids[0].Vec(), vec(2, 2)) {
		t.Fatal("incorrect vec in c1: ", cm.Centroids[0].Vec())
	}
	if !vecEq(cm.Centroids[1].Vec(), vec(4, 4)) {
		t.Fatal("incorrect vec in c2: ", cm.Centroids[0].Vec())
	}
	if !vecEq(cm.Vec(), vec(3, 3)) {
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
	if vecIn(vec(1, 9), dps2Vecs(c1dps)) {
		t.Fatalf("c1dps still contains vec with bad fit.")
	}

	// Confirm that dp8 is no longer in c2 (moved to c1).
	if vecIn(vec(1, 3), dps2Vecs(c2dps)) {
		t.Fatalf("c1dps still contains vec with bad fit.")
	}
}

func TestDistributeDataPointsInternal(t *testing.T) {
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
	cm.DistributeDataPointsInternal(2)

	c1dps := c1.DrainUnordered(9) // Convenience
	c2dps := c2.DrainUnordered(9) // Convenience

	if len(c1dps) != 4 {
		t.Fatalf("incorrect dp amount in c1: %v\n", len(c1dps))
	}
	if len(c2dps) != 4 {
		t.Fatalf("incorrect dp amount in c2: %v\n", len(c2dps))
	}
	// Confirm that dp4 is no longer in c1 (moved to c2).
	if vecIn(vec(1, 9), dps2Vecs(c1dps)) {
		t.Fatalf("c1dps still contains vec with bad fit.")
	}

	// Confirm that dp8 is no longer in c2 (moved to c1).
	if vecIn(vec(1, 3), dps2Vecs(c2dps)) {
		t.Fatalf("c1dps still contains vec with bad fit.")
	}
}

func TestKNNLookupNoDrain(t *testing.T) {
	if !t.Run("Test Dependency 1", TestMoveVector) {
		t.Fatalf("Expected TestMoveVector to work, it did not.")
	}

	c1 := newCentroid(vec(1, 1))
	c1.AddDataPoint(dp(vec(1, 2), 0)) // dp1.
	c1.AddDataPoint(dp(vec(1, 3), 0)) // dp2.

	c2 := newCentroid(vec(1, 5))
	c2.AddDataPoint(dp(vec(1, 6), 0)) // dp3.
	c2.AddDataPoint(dp(vec(1, 7), 0)) // dp4

	cm := newCentroidManager(vec(0, 0))
	cm.Centroids = []common.Centroid{c1, c2}
	cm.MoveVector() // For auto-adjusting vec test.

	// vec(1, 5.7) is closest to dp3 in c2.
	dps := cm.KNNLookup(vec(1, 5.7), 1, true)

	if c1.LenDP() != 2 {
		t.Fatalf("unexpected dp drain in c1: len=%v", c1.LenDP())
	}

	if c2.LenDP() != 1 {
		t.Fatalf("unexpected dp drain in c2: len=%v", c2.LenDP())
	}

	if len(dps) != 1 {
		t.Fatalf("unexpected result len: %v (want 1)", len(dps))
	}
	if !vecEq(dps[0].Vec(), vec(1, 6)) { // vec(1,6)=dp3
		t.Fatalf("unexpected result vec: %v", dps[0].Vec())
	}

	// Auto-adjust vec test.
	vecBkp := vec(cm.vec...)
	cm.MoveVector()
	if !vecEq(vecBkp, cm.vec) {
		t.Fatalf("auto-adjusted cm vec is incorrect. want %v, have %v", cm.vec, vecBkp)
	}
}

func TestNearestCentroid(t *testing.T) {
	c1 := newCentroid(vec(1, 2))
	c2 := newCentroid(vec(1, 3))
	c3 := newCentroid(vec(1, 4))

	cm := newCentroidManager(vec(0, 0))
	cm.Centroids = []common.Centroid{c1, c2, c3}

	centroids, ok := cm.NearestCentroids(vec(1, 5), 1, true) // nearest c3.
	if !ok {
		t.Fatal("didn't get any centroid")
	}
	if len(centroids) != 1 {
		t.Fatalf("got incorrect amt of centroids: %v", len(centroids))
	}
	if len(cm.Centroids) != 2 {
		t.Fatalf("incorrect cm centroids remainder: %v", len(cm.Centroids))
	}
	c := centroids[0]
	if !vecEq(c.Vec(), c3.Vec()) {
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
	if !t.Run("Test Dependency 1", TestMoveVector) {
		t.Fatalf("Expected TestMoveVector to work, it did not.")
	}

	c1 := newCentroid(vec(1, 1))
	c2 := newCentroid(vec(1, 9))
	c3 := newCentroid(vec(1, 2)) // closest to c1.

	// Vecs here only matter for vector auto-adjust test.
	c1.AddDataPoint(dp(vec(1, 3), 0))
	c2.AddDataPoint(dp(vec(2, 5), 0))
	c3.AddDataPoint(dp(vec(9, 8), 0))
	c3.AddDataPoint(dp(vec(1, 7), 0))

	cm := newCentroidManager(vec(0, 0))
	cm.Centroids = []common.Centroid{c1, c2, c3}
	cm.MoveVector() // For auto-adjust vec test.

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

	// Auto-adjust vec test.
	vecBkp := vec(cm.vec...)
	cm.MoveVector()
	if !vecEq(vecBkp, cm.vec) {
		t.Fatalf("auto-adjusted cm vec is incorrect. want %v, have %v", cm.vec, vecBkp)
	}
}
