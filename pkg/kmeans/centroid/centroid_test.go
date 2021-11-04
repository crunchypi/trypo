package centroid

import (
	"testing"
	"time"
	"trypo/pkg/kmeans/common"
	"trypo/pkg/mathutils"
	"trypo/pkg/searchutils"
)

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

// helper for creating a data point.
func dp(v []float64, sleepUnits int) common.DataPoint {
	_dp := common.DataPoint{Vec: v}

	if sleepUnits > 0 {
		_dp.Expires = time.Now().Add(_SLEEPUNIT * time.Duration(sleepUnits))
		_dp.ExpireEnabled = true
	}
	return _dp
}

func dps2Vecs(dps []common.DataPoint) [][]float64 {
	res := make([][]float64, len(dps))
	for i, dp := range dps {
		res[i] = dp.Vec
	}
	return res
}

func sleep() {
	time.Sleep(_SLEEPUNIT)
}

// Helper for auto-configuring a centroid and search funcs.
func newCentroid(vec []float64) Centroid {
	c, ok := NewCentroid(NewCentroidArgs{
		InitVec:       vec,
		InitCap:       0,
		KNNSearchFunc: searchutils.KNNCos,
		KFNSearchFunc: searchutils.KFNCos,
	})

	if !ok {
		panic("failed test configuration")
	}
	return c
}

/*
--------------------------------------------------------------------------------
Proper test section.
--------------------------------------------------------------------------------
*/

// Unexported addDataPoint
func TestAddDataPoint(t *testing.T) {
	c := newCentroid(vec(1, 1)) // vec here doesn't matter.

	// Add first dp, new c.vec should be 3,3
	c.addDataPoint(dp(vec(3, 3), 0))
	if !vecEq(c.Vec(), vec(3, 3)) {
		t.Fatalf("did not adjust internal vector correctly: %v", c.Vec())
	}

	// (3,3) + (5,5) = (4,4)
	c.addDataPoint(dp(vec(5, 5), 0))
	if !vecEq(c.Vec(), vec(4, 4)) {
		t.Fatalf("did not adjust internal vector correctly: %v", c.Vec())
	}

}

func TestAddDataPointExported(t *testing.T) {
	c := newCentroid(vec(1, 1, 1))
	if !c.AddDataPoint(dp(vec(2, 2, 2), 0)) {
		t.Fatalf("failed to add datapoint")
	}

	if len(c.DataPoints) != 1 {
		t.Error("didn't add datapoint")
	}
}

func TestRmDataPoint(t *testing.T) {
	c := newCentroid(vec(0, 0))
	c.DataPoints = []common.DataPoint{
		dp(vec(2, 2), 0), // dp1.
		dp(vec(4, 4), 0), // dp2.
		dp(vec(6, 6), 0), // dp3.
		dp(vec(8, 8), 0), // dp4.
	}
	c.vec = vec(5, 5) // Mean of all dps.

	c.rmDataPoint(3) // dp4.
	if !vecEq(c.Vec(), vec(4, 4)) {
		t.Fatalf("did not adjust internal vec correctly (no. 1): %v", c.Vec())
	}

	c.rmDataPoint(0) // dp1.
	if !vecEq(c.Vec(), vec(5, 5)) {
		t.Fatalf("did not adjust internal vec correctly (no. 2): %v", c.Vec())
	}

	c.rmDataPoint(0) // dp2, note index shifted
	if !vecEq(c.Vec(), vec(6, 6)) {
		t.Fatalf("did not adjust internal vec correctly (no. 3): %v", c.Vec())
	}

	c.rmDataPoint(0) // dp3, note index shifted
	if !vecEq(c.Vec(), vec(6, 6)) {
		t.Fatalf("did not adjust internal vec correctly (no. 3): %v", c.Vec())
	}
	if len(c.DataPoints) != 0 {
		t.Errorf("didn't remove all dps: %v", len(c.DataPoints))
	}
}

func TestDataPointVecGenerator(t *testing.T) {
	c := newCentroid(vec(1, 1))
	c.DataPoints = []common.DataPoint{
		dp(vec(1, 2), 0),
		dp(vec(1, 3), 1), // Expire so gen returns 1 thing.
	}

	sleep()
	gen := c.dataPointVecGenerator()
	v, _ := gen()
	if !vecEq(v, vec(1, 2)) {
		t.Fatalf("generator produced incorrect res: %v", v)
	}
	_, cont := gen()
	if cont {
		t.Fatalf("second generator call signals continue")
	}
	if len(c.DataPoints) != 1 {
		t.Fatalf("generator didn't expire one datapoint")
	}
	if !vecEq(c.DataPoints[0].Vec, vec(1, 2)) {
		t.Fatalf("generator expired incorrect datapoint")
	}
}

func TestDrainUnordered(t *testing.T) {
	c := newCentroid(vec(1, 1))
	c.DataPoints = []common.DataPoint{
		dp(vec(2, 2), 1), // Should be dropped since it expires.
		dp(vec(3, 3), 0), // Should be returned.
		dp(vec(4, 4), 0), // Remainder if c.DrainUnordered(1)
	}

	sleep()
	dps := c.DrainUnordered(1)
	if len(c.DataPoints) != 1 {
		t.Fatalf("incorrect amt of dps in centroid: %v", c.DataPoints)
	}
	if !vecEq(c.DataPoints[0].Vec, vec(4, 4)) {
		t.Fatalf("incorrect dp remainder in centroid: %v", c.DataPoints)
	}
	if len(dps) != 1 {
		t.Fatalf("result should be of len 1: %v", dps)
	}
	if !vecEq(dps[0].Vec, vec(3, 3)) {
		t.Fatalf("inorrect drain result: %v", dps)
	}
}

func TestDrainOrdered(t *testing.T) {
	c := newCentroid(vec(1, 1))
	c.DataPoints = []common.DataPoint{
		// Should be dropped since it expires, even though it's furthest
		// (cosine similarity) away from the centroid .
		dp(vec(1, 4), 1), // DP1
		// Should be left alone (if arg to c.DrainOrdered is 1), as it is
		// closer to the centroid (cosine simi), compared to the next dp.
		dp(vec(1, 2), 0), // DP2
		// Should be returned since it's furthest away (cosine similarity)
		// from the centroid, except the one that's expired and dropped.
		dp(vec(1, 3), 0), // DP3
	}

	sleep()
	dps := c.DrainOrdered(1)
	if len(c.DataPoints) != 1 {
		t.Fatalf("drain issue, incorrect amt of dps in centroid: %v", c.DataPoints)
	}
	if !vecEq(c.DataPoints[0].Vec, vec(1, 2)) { // DP3.
		t.Fatalf("drain issue, incorrect dp remainder in centroid: %v", c.DataPoints)
	}
	if len(dps) != 1 {
		t.Fatalf("drain issue, result should be of len 1: %v", dps)
	}
	if !vecEq(dps[0].Vec, vec(1, 3)) { // DP2.
		t.Fatalf("drain issue: inorrect drain result: %v", dps)
	}
}

func TestExpire(t *testing.T) {
	c := newCentroid(vec(1, 1))
	c.DataPoints = []common.DataPoint{
		// Should be dropped since it expires.
		dp(vec(2, 2), 1),
		// Should be left alone since it doesn't expire right away.
		dp(vec(3, 3), 2),
		// Should be dropped since it expires.
		dp(vec(4, 4), 1),
	}

	sleep()
	c.Expire()
	if len(c.DataPoints) != 1 {
		t.Fatalf("expire issue, incorrect amt of dps in centroid: %v", c.DataPoints)
	}
	if !vecEq(c.DataPoints[0].Vec, vec(3, 3)) {
		t.Fatalf("expire issue, incorrect val remains in centroid: %v", c.DataPoints)
	}
}

func TestMoveVector(t *testing.T) {
	c := newCentroid(vec(0, 0))
	c.DataPoints = []common.DataPoint{
		dp(vec(1, 1), 0),
		dp(vec(3, 3), 0),
	}
	c.MoveVector()
	if !vecEq(c.vec, vec(2, 2)) {
		t.Error("incorrect mean")
	}
}

func TestDistributeDataPoints(t *testing.T) {
	// Receivers.
	c1 := newCentroid(vec(1, 2)) // Increasing angles (for cosine search).
	c2 := newCentroid(vec(1, 3)) // Increasing angles (for cosine search).

	// Sender.
	c0 := newCentroid(vec(1, 1))
	c0.DataPoints = []common.DataPoint{
		dp(c1.vec, 0), // dp1, This should be sent to c1.
		dp(c2.vec, 0), // dp2, This should be sent to c2.
	}

	receivers := []common.DataPointReceiver{&c1, &c2}
	c0.DistributeDataPoints(2, receivers)

	if len(c0.DataPoints) != 0 {
		t.Fatal("sender didn't distribute all dp")
	}

	if len(c1.DataPoints) != 1 {
		t.Fatalf("reciever 1 didn't recieve a dp")
	}

	if len(c2.DataPoints) != 1 {
		t.Fatalf("reciever 2 didn't recieve a dp")
	}

	if !vecEq(c1.DataPoints[0].Vec, vec(1, 2)) { // dp1.
		t.Fatalf("reciever 1 didn't get the correct dp")
	}

	if !vecEq(c2.DataPoints[0].Vec, vec(1, 3)) { // dp2.
		t.Fatalf("reciever 2 didn't get the correct dp")
	}
}

func TestKNNLookup(t *testing.T) {
	c := newCentroid(vec(0, 0, 0))

	c.DataPoints = []common.DataPoint{
		// Increasing angles for each vector.
		dp(vec(1, 2, 3), 0), // dp1.
		dp(vec(1, 3, 4), 0), // dp2.
	}
	// vec(1,1,1) is closest to dp1.
	dp := c.KNNLookup(vec(1, 1, 1), 1, true)

	if len(dp) != 1 {
		t.Fatal("incorrect result length/amount")
	}
	if !vecEq(dp[0].Vec, vec(1, 2, 3)) { // dp1.
		t.Fatal("incorrect result value")
	}
	if len(c.DataPoints) != 1 {
		t.Fatal("centroid didn't drain")
	}
}
