package rpc

import (
	"fmt"
	"testing"
	"time"
	"trypo/pkg/kmeans/centroid"
	"trypo/pkg/kmeans/centroidmanager"
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

It is mostly aliased vector tools, convenient creation of DataPoint,
kmeans.Centroid, kmeans.CentroidManager and the KMeansServer.
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
func dp(v []float64, sleepUnits int) DataPoint {
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

func newCentroid(vec []float64) *Centroid {
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
	return &centroid
}

func newCentroidManager(vec []float64) *CentroidManager {
	args := centroidmanager.NewCentroidManagerArgs{
		InitVec:             vec,
		InitCap:             0,
		CentroidDPThreshold: 10,
		KNNSearchFunc:       _knnSearchFunc,
		KFNSearchFunc:       _kfnSearchFunc,
	}
	cm, ok := centroidmanager.NewCentroidManager(args)
	if !ok {
		panic("couldn't setup CentroidManager for test")
	}
	return &cm
}

func newKMeansServer(addr string) *KMeansServer {
	slots := make(map[string]*CManagerSlot)
	table := CManagerTable{slots: slots}

	return &KMeansServer{
		addr:                       addr,
		Table:                      &table,
		CentroidManagerFactoryFunc: newCentroidManager,
	}
}

func sleep() {
	time.Sleep(_SLEEPUNIT)
}

/*
--------------------------------------------------------------------------------
Section for utils 2, here lies a type (and methods) that is used to
test/simulate a test network.
--------------------------------------------------------------------------------
*/
type addr = string
type tNetwork struct {
	nodes     map[addr]*KMeansServer
	stopFuncs map[addr]func()
}

func newTNetwork(addrs []addr) tNetwork {
	nodes := make(map[addr]*KMeansServer, len(addrs))
	stopFuncs := make(map[addr]func())

	for _, addr := range addrs {
		s := newKMeansServer(addr)
		nodes[addr] = s

		stopFunc, startErr := StartListen(s)
		if startErr != nil {
			panic(fmt.Sprintf("couldn't start server on addr %v", addr))
		}
		stopFuncs[addr] = stopFunc
	}

	return tNetwork{nodes: nodes, stopFuncs: stopFuncs}
}

func (tn *tNetwork) stop() {
	for _, f := range *&tn.stopFuncs {
		f()
	}
}

func (tn *tNetwork) reset() {
	for _, node := range *&tn.nodes {
		slots := make(map[string]*CManagerSlot)
		table := CManagerTable{slots: slots}
		node.Table = &table
	}
}

// convenience
func (tn *tNetwork) unwrap(addr, namespace string) *CentroidManager {
	node := tn.nodes[addr]
	return node.Table.slots[namespace].cManager
}

// One address per node in a tNetwork instance (next var).
var addrs = []addr{"localhost:3000", "localhost:3001", "localhost:3002"}

// A test network for all tests, this should be cleaned in each test,
// so use 'defer network.reset()' or something like that. It is
// cleanly shut down with the last 'test' in this file: 'TestCleanup(..)'
var network tNetwork

func init() {
	network = newTNetwork(addrs)
}

/*
--------------------------------------------------------------------------------
Section for actual tests.
--------------------------------------------------------------------------------
*/

func TestVec(t *testing.T) {
	// Boilerplate.
	defer network.reset()
	namespace := "test"
	addr := addrs[0]

	// Test setup; simply setup a remote node where the
	// vec of CentroidManager is vec1.
	vec1 := vec(3, 4)
	slot := CManagerSlot{cManager: newCentroidManager(vec1)}
	network.nodes[addr].Table.slots[namespace] = &slot

	// Validation.
	var err error
	vec2 := KMeansClient(addr, namespace, &err).Vec()

	if err != nil {
		t.Fatalf("client err: %v", err)
	}

	if !vecEq(vec1, vec2) {
		t.Fatalf("got incorrect vector. want %v, got %v", vec1, vec2)
	}
}

func TestAddDataPoint(t *testing.T) {
	// Boilerplate.
	defer network.reset()
	namespace := "test"
	addr := addrs[0]

	// Test setup; simply setup a remote node.
	vec1 := vec(1, 5)
	cm := newCentroidManager(vec1)
	slot := CManagerSlot{cManager: cm}
	network.nodes[addr].Table.AddSlot(namespace, &slot)

	// Validation.
	var err error
	addOK := KMeansClient(addr, namespace, &err).AddDataPoint(dp(vec1, 0))

	if err != nil {
		t.Fatalf("client err: %v", err)
	}

	if !addOK {
		t.Fatalf("got unexpected resp: %v", addOK)
	}

	vec2 := cm.Centroids[0].DataPoints[0].Vec
	if !vecEq(vec1, vec2) {
		t.Fatalf("remote vector incorrect")
	}
}

func TestDrainUnordered(t *testing.T) {
	// Boilerplate.
	defer network.reset()
	namespace := "test"
	addr := addrs[0]

	// Test setup; setup remote node with one centroid containing two dps.
	// The cm.DrainUnordered calls the method with the same name on the
	// centroid, and that should drain the internal dp slice from left to
	// right, so a dp with vec1 should be drained first.
	vec1 := vec(1, 3)
	vec2 := vec(1, 5)
	cm := newCentroidManager(vec1)
	slot := CManagerSlot{cManager: cm}
	network.nodes[addr].Table.AddSlot(namespace, &slot)
	cm.AddDataPoint(dp(vec1, 0))
	cm.AddDataPoint(dp(vec2, 0))

	// Validation.
	var err error
	dps := KMeansClient(addr, namespace, &err).DrainUnordered(1)

	if err != nil {
		t.Fatalf("client err: %v", err)
	}

	if len(dps) != 1 {
		t.Fatalf("returned dps len unexpected: %v", len(dps))
	}

	if !vecEq(dps[0].Vec, vec1) {
		t.Fatalf("unexpected vec of returned dp: want %v, got %v", vec1, vec2)
	}

	remotedps := cm.Centroids[0].DataPoints
	if len(remotedps) != 1 {
		t.Fatalf("remote dp remainder incorrect: len=%v", len(remotedps))
	}

	if !vecEq(remotedps[0].Vec, vec2) {
		t.Fatalf("remainding remote dp incorrect: vec=%v", dps[0].Vec)
	}
}

func TestDrainOrdered(t *testing.T) {
	// Boilerplate.
	defer network.reset()
	namespace := "test"
	addr := addrs[0]

	// Test setup; setup remote node with one centroid containing a
	// few datapoints. When doing cm.AddDataPoint, the internal vec
	// should be automatically updated, so 4 dps are added, where 3
	// of them are the same, such that the fourth is the odd one out.
	// The remote cm.DrainOrdered call should return that oddball.
	cm := newCentroidManager(vec(0, 0))
	slot := CManagerSlot{cManager: cm}
	network.nodes[addr].Table.AddSlot(namespace, &slot)

	dp1 := dp(vec(1, 5), 0)
	dp2 := dp(vec(1, 9), 0)
	dp3 := dp(vec(1, 9), 0)
	dp4 := dp(vec(1, 9), 0)

	for _, dp := range []DataPoint{dp1, dp2, dp3, dp4} {
		cm.AddDataPoint(dp)
	}

	// Validation.
	var err error
	dps := KMeansClient(addr, namespace, &err).DrainUnordered(1)

	if err != nil {
		t.Fatalf("client err: %v", err)
	}

	if len(dps) != 1 {
		t.Fatalf("returned dps len unexpected: %v", len(dps))
	}

	if !vecEq(dps[0].Vec, dp1.Vec) {
		t.Fatalf("unexpected vec of returned dp: want %v, got %v", dps[0].Vec, dp1.Vec)
	}

	remotedps := cm.Centroids[0].DataPoints
	if len(remotedps) != 3 {
		t.Fatalf("remote dp remainder incorrect: len=%v", len(remotedps))
	}
}

func TestExpire(t *testing.T) {
	// Boilerplate.
	defer network.reset()
	namespace := "test"
	addr := addrs[0]

	// Test setup; setup remote node with one dp which expires.
	vec1 := vec(1, 5)
	cm := newCentroidManager(vec1)
	slot := CManagerSlot{cManager: cm}
	network.nodes[addr].Table.AddSlot(namespace, &slot)
	cm.AddDataPoint(dp(vec1, 1)) // Note expiration enables.
	sleep()

	// Test validation.
	var err error
	KMeansClient(addr, namespace, &err).Expire()

	if err != nil {
		t.Fatalf("client err: %v", err)
	}

	remotedps := cm.Centroids[0].DataPoints
	if len(remotedps) != 0 {
		t.Fatalf("didn't expire+remove remote dp")
	}
}

func TestLenDP(t *testing.T) {
	// Boilerplate.
	defer network.reset()
	namespace := "test"
	addr := addrs[0]

	// Test setup; setup remote node with one dp.
	vec1 := vec(1, 5)
	cm := newCentroidManager(vec1)
	slot := CManagerSlot{cManager: cm}
	network.nodes[addr].Table.AddSlot(namespace, &slot)
	cm.AddDataPoint(dp(vec1, 0))

	// Validation.
	var err error
	lendp := KMeansClient(addr, namespace, &err).LenDP()

	if err != nil {
		t.Fatalf("client err: %v", err)
	}

	if lendp != 1 {
		t.Fatalf("incorrect remote dp len: %v", lendp)
	}
}

func TestMemTrim(t *testing.T) {
	// Boilerplate.
	defer network.reset()
	namespace := "test"
	addr := addrs[0]

	// Test setup; setup remote node where the CentroidManager
	// has one Centroid (having no dps). Calling remote cm.MemTrim
	// should remove all centroids that have no dps...
	c1 := newCentroid(vec(1, 1))
	cm := newCentroidManager(vec(0, 0))
	cm.Centroids = []*Centroid{c1}
	slot := CManagerSlot{cManager: cm}
	network.nodes[addr].Table.AddSlot(namespace, &slot)

	// Validation.
	var err error
	KMeansClient(addr, namespace, &err).MemTrim()

	if err != nil {
		t.Fatalf("client err: %v", err)
	}

	if len(cm.Centroids) != 0 {
		t.Fatalf("memtrim should have removed centroids")
	}
}

func TestMoveVector(t *testing.T) {
	// Boilerplate.
	defer network.reset()
	namespace := "test"
	addr := addrs[0]

	// Test setup; setup a remote node with two centroids, each
	// having a mean vec of (2,2). Calling a remote cm.MoveVector
	// should find that mean and set it to the internal vec.
	c1 := newCentroid(vec(1, 1))
	c2 := newCentroid(vec(3, 3))

	cm := newCentroidManager(vec(0, 0))
	cm.Centroids = []*Centroid{c1, c2}

	slot := CManagerSlot{cManager: cm}
	network.nodes[addr].Table.AddSlot(namespace, &slot)

	// Validation.
	var err error
	ok := KMeansClient(addr, namespace, &err).MoveVector()

	if err != nil {
		t.Fatalf("client err: %v", err)
	}

	if !ok {
		t.Fatalf("unexpected resp: %v", ok)
	}

	if !vecEq(cm.Vec(), vec(2, 2)) {
		t.Fatalf("incorrect remote vec: want %v, have %v", vec(2, 2), cm.Vec())
	}
}

func TestDistributeDataPointsFast(t *testing.T) {
	// Boilerplate.
	defer network.reset()
	namespace := "test"

	// Test setup:
	// The CentroidManager and datapoint setup below is set up such
	// that dp4 is in cm1 but is actually closer to cm2. Likewise, dp8
	// is in cm2 but is closer to cm1.

	// Node 1.
	cm1 := newCentroidManager(vec(0, 0))
	slot1 := CManagerSlot{cManager: cm1}
	addr1 := addrs[0]
	network.nodes[addr1].Table.AddSlot(namespace, &slot1)

	// Node 2.
	cm2 := newCentroidManager(vec(0, 0))
	slot2 := CManagerSlot{cManager: cm2}
	addr2 := addrs[1]
	network.nodes[addr2].Table.AddSlot(namespace, &slot2)

	// Mean: (1, 4.5) : Small angle.
	cm1.AddDataPoint(dp(vec(1, 3), 0)) // dp1
	cm1.AddDataPoint(dp(vec(1, 3), 0)) // dp2
	cm1.AddDataPoint(dp(vec(1, 3), 0)) // dp3
	cm1.AddDataPoint(dp(vec(1, 9), 0)) // dp4: closest to c2.

	// Mean: (1, 7.5) : Large angle
	cm2.AddDataPoint(dp(vec(1, 9), 0)) // dp5
	cm2.AddDataPoint(dp(vec(1, 9), 0)) // dp6
	cm2.AddDataPoint(dp(vec(1, 9), 0)) // dp7
	cm2.AddDataPoint(dp(vec(1, 3), 0)) // dp8: closest to c1.

	// Validation.
	addrs := []string{addr1, addr2}
	for i, addr := range addrs { // Move dps between nodes.
		var err error
		KMeansClient(addr, namespace, &err).DistributeDataPointsFast(addrs, 1)

		if err != nil {
			t.Fatalf("client %v err: %v", i, err)
		}
	}

	c1dps := cm1.DrainUnordered(9) // Convenience
	c2dps := cm2.DrainUnordered(9) // Convenience

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
		t.Fatalf("c2dps still contains vec with bad fit.")
	}
}

func TestDistributeDataPointsAccurate(t *testing.T) {
	// Boilerplate.
	defer network.reset()
	namespace := "test"

	// Test description:
	// The only datapoint (dp1) fits snugly within its _CentroidManager_
	// instance (cm1) but doesn't fit at all within its _Centroid_ instance
	// (c1). DistributeDataPointsFast (the other variant) hast _CentroidManager_
	// accuracy, so the problem won't be solved. The method tested here
	// (DistributeDataPointsAccurate) has _Centroid_ accuracy (i.e grater
	// granularity) and should solve this problem (move dp1->c2).
	// A note; there is only one dp and it should be moved one way. Using
	// two dps (one in c1 and one in c2) for a symmetric two-way move is
	// not checked because the setup is a bit tricky, as moving dps will
	// change the vectors of centroids and centroidmanagers...

	// More convoluted and accurate description:
	// dp1 is in c1 (centroid 1) and cm1 (centroidmanager 1). Its vec
	// is exactly the same as cm1, so using DistributeDataPointsFast
	// should not change anything. The problem is, however, that it is
	// not similar at all to c1, but is has the exact vec as c2, contained
	// in cm2...

	// Setup node 1.
	c1 := newCentroid(vec(1, 1))
	c1.DataPoints = []DataPoint{
		dp(vec(1, 9), 0), // dp1 (misfit in c1).
	}
	cm1 := newCentroidManager(vec(1, 9)) // closet to dp1.
	cm1.Centroids = []*Centroid{c1}
	addr1 := addrs[0]
	network.nodes[addr1].Table.AddSlot(namespace, &CManagerSlot{cManager: cm1})

	// Setup node 2.
	c2 := newCentroid(vec(1, 9))
	cm2 := newCentroidManager(vec(1, 1))
	cm2.Centroids = []*Centroid{c2}
	addr2 := addrs[1]
	network.nodes[addr2].Table.AddSlot(namespace, &CManagerSlot{cManager: cm2})

	// Validation.
	var err error
	client := KMeansClient(addr1, namespace, &err)          // send from misfit.
	client.DistributeDataPointsAccurate([]string{addr2}, 1) // send to fit.

	if err != nil {
		t.Fatalf("net err for %v: %v", addrs[0], err)
	}

	// Again, dp1 fits in cm1 but not in c1, so should have been moved to c2.
	if len(c1.DataPoints) != 0 {
		t.Fatalf("incorrect dp amount in c1: %v\n", len(c1.DataPoints))
	}
	if len(c2.DataPoints) != 1 {
		t.Fatalf("incorrect dp amount in c2: %v\n", len(c2.DataPoints))
	}
}

func TestDistributeDataPointsInternal(t *testing.T) {
	// Boilerplate.
	defer network.reset()
	namespace := "test"
	addr := addrs[0]

	// Test setup:
	// The centroid and datapoint setup below is set up such that
	// dp4 is in c1 but is actually closer to c2. Likewise, dp8
	// is in c2 but is closer to c1.

	// These 2 vectors don't matter, they are assumed to be auto,
	// adjusted while adding dps. That is why there are a lot
	// of added dps below (so dp4 and dp8 are oddballs).
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
	cm.Centroids = []*centroid.Centroid{c1, c2}
	slot := CManagerSlot{cManager: cm}
	network.nodes[addr].Table.AddSlot(namespace, &slot)

	// Validation.
	var err error
	KMeansClient(addr, namespace, &err).DistributeDataPointsInternal(99)

	if err != nil {
		t.Fatalf("client err: %v", err)
	}

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
		t.Fatalf("c2dps still contains vec with bad fit.")
	}
}

func TestKNNLookup(t *testing.T) {
	// Boilerplate.
	defer network.reset()
	namespace := "test"
	addr := addrs[0]

	// Test setup; setup remote node with a couple of dps, the query
	// (vec) should be closest to one of them.
	cm := newCentroidManager(vec(0, 0))
	slot := CManagerSlot{cManager: cm}
	network.nodes[addr].Table.AddSlot(namespace, &slot)

	dp1 := dp(vec(1, 2), 0)
	dp2 := dp(vec(1, 9), 0)

	cm.AddDataPoint(dp1)
	cm.AddDataPoint(dp2)

	queryVec := vec(1, 3) // closest to dp1

	// Validation.
	var err error
	dps := KMeansClient(addr, namespace, &err).KNNLookup(queryVec, 1, true)

	if err != nil {
		t.Fatalf("client err: %v", err)
	}

	if len(dps) != 1 {
		t.Fatalf("unexpected dps len: %v", len(dps))
	}

	if !vecEq(dps[0].Vec, dp1.Vec) {
		t.Fatalf("unexpected dp return: vec=%v", dps[0].Vec)
	}
}

func TestNearestCentroid(t *testing.T) {
	// Boilerplate.
	defer network.reset()
	namespace := "test"
	addr := addrs[0]

	// Test setup; setup remote node with a couple of centroids,
	// the query (vec) should be closest to one of them.
	d1 := dp(vec(1, 4), 0)
	d2 := dp(vec(1, 9), 0)

	c1 := newCentroid(d1.Vec)
	c1.AddDataPoint(d1)

	c2 := newCentroid(d2.Vec)
	c2.AddDataPoint(d2)

	cm := newCentroidManager(vec(0, 0))
	cm.Centroids = []*Centroid{c1, c2}
	slot := CManagerSlot{cManager: cm}
	network.nodes[addr].Table.AddSlot(namespace, &slot)

	queryVec := vec(1, 3) // Closest to c1.

	// Validate.
	var err error
	cs, _ := KMeansClient(addr, namespace, &err).NearestCentroids(queryVec, 1, true)

	if err != nil {
		t.Fatalf("client err: %v", err)
	}

	if len(cs) != 1 {
		t.Fatalf("got incorrect centroids as resp: %v", len(cs))
	}

	if !vecEq(cs[0].Vec(), d1.Vec) {
		t.Fatalf("got incorrect centroid with vec %v", cs[0].Vec())
	}

	if len(cm.Centroids) != 1 {
		t.Fatalf("remote CentroidManager didn't lose a centroid")
	}

	v := cm.Centroids[0].Vec()
	if !vecEq(v, vec(1, 9)) {
		t.Fatalf("unexpected remote cm centroid remainder with vec %v", v)
	}
}

func TestNearestCentroidVec(t *testing.T) {
	// Boilerplate.
	defer network.reset()
	namespace := "test"
	addr := addrs[0]

	// Test setup; remote node with two centroids, where one is closest
	// to the queryVec.
	queryVec := vec(1, 9)
	c1 := newCentroid(vec(1, 1))
	c2 := newCentroid(queryVec)
	cm := newCentroidManager(vec(0, 0))
	cm.Centroids = []*Centroid{c1, c2}
	slot := CManagerSlot{cManager: cm}
	network.nodes[addr].Table.AddSlot(namespace, &slot)

	// Validation.
	var err error
	resp := KMeansClient(addr, namespace, &err).NearestCentroidVec(queryVec)

	if err != nil {
		t.Fatalf("client err: %v", err)
	}
	if resp == nil {
		t.Fatalf("nil resp vec")
	}
	if !vecEq(queryVec, resp) {
		t.Fatalf("unexpected resp: %v", resp)
	}

}

func TestSplitCentroids(t *testing.T) {
	// Boilerplate.
	defer network.reset()
	namespace := "test"
	addr := addrs[0]

	// Test setup: Setup a remote CentroidManager containing one Centroid with
	// 4 dps. The kmeansClient.SplitCentroids method accepts a range; if a
	// Centroid in a CentroidManager has an amount of dps that falls within
	// that range, then that Centroid will be split in half.
	dps := []common.DataPoint{
		dp(vec(1), 0),
		dp(vec(1), 0),
		dp(vec(1), 0),
		dp(vec(1), 0),
	}
	c1 := newCentroid(dps[0].Vec)
	for _, dp := range dps {
		c1.AddDataPoint(dp)
	}
	cm := newCentroidManager(vec(0, 0))
	cm.Centroids = []*Centroid{c1}
	slot := CManagerSlot{cManager: cm}
	network.nodes[addr].Table.AddSlot(namespace, &slot)

	// Validation:
	var err error
	KMeansClient(addr, namespace, &err).SplitCentroids(0, len(dps)+1)

	if err != nil {
		t.Fatalf("client err: %v", err)
	}

	if len(cm.Centroids) != 2 {
		t.Fatalf("incorrect centroid count after split: %v", len(cm.Centroids))
	}

	l1 := cm.Centroids[0].LenDP()
	l2 := cm.Centroids[1].LenDP()
	if l1 != 2 || l2 != 2 {
		t.Fatal("uneven datapoint distribution after split:", l1, l2)
	}
}

func TestMergeCentroids(t *testing.T) {
	// Boilerplate.
	defer network.reset()
	namespace := "test"
	addr := addrs[0]

	// Test setup: Setup a remote CentroidManager with 3 Centroids where:
	// - c1 len(dp) = 1
	// - c2 len(dp) = 1
	// - c3 len(dp) = 2
	// The merge condition will be that a centroid with 2 dps (so c3) is
	// merged with another centroid that is closest to it (c1).
	c1 := newCentroid(vec(1, 1))
	c2 := newCentroid(vec(1, 9))
	c3 := newCentroid(vec(1, 2)) // closest to c1.

	c1.AddDataPoint(dp(vec(1, 1), 0))
	c2.AddDataPoint(dp(vec(1, 1), 0))
	c3.AddDataPoint(dp(vec(1, 1), 0))
	c3.AddDataPoint(dp(vec(1, 1), 0))

	cm := newCentroidManager(vec(0, 0))
	cm.Centroids = []*centroid.Centroid{c1, c2, c3}
	slot := CManagerSlot{cManager: cm}
	network.nodes[addr].Table.AddSlot(namespace, &slot)

	// Validation.
	var err error
	KMeansClient(addr, namespace, &err).MergeCentroids(1, 3)

	if err != nil {
		t.Fatalf("client err: %v", err)
	}

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

func TestStealCentroids(t *testing.T) {
	// Boilerplate.
	defer network.reset()
	namespace := "test"

	c1 := newCentroid(vec(1, 1))
	c1.DataPoints = []DataPoint{
		dp(vec(1, 1), 0),
		dp(vec(1, 1), 0),
	}

	c2 := newCentroid(vec(1, 3))
	c2.DataPoints = []DataPoint{
		dp(vec(1, 1), 0),
		dp(vec(1, 1), 0),
	}

	cm1 := newCentroidManager(vec(1, 1)) // Stolen from.
	cm1.Centroids = []*Centroid{c1, c2}
	network.nodes[addrs[1]].Table.AddSlot(namespace, &CManagerSlot{cManager: cm1})

	// Vec here is closest to c2.
	cm2 := newCentroidManager(vec(1, 3)) // Stealer.
	network.nodes[addrs[0]].Table.AddSlot(namespace, &CManagerSlot{cManager: cm2})

	// Validate.
	var err error
	client := KMeansClient(addrs[0], namespace, &err)
	n, ok := client.StealCentroids(addrs[1], 2)

	if err != nil {
		t.Fatalf("net err for %v: %v", addrs[0], err)
	}
	if !ok {
		t.Fatal("got false from call StealCentroids call")
	}
	if n != 2 {
		t.Fatalf("unexpected total dp transfer. want %v, got %v", 2, n)
	}
	if len(cm1.Centroids) != 1 {
		t.Fatalf("unexpected centroid amt in c1: want %v, have %v", 1, len(cm1.Centroids))
	}
	if len(cm2.Centroids) != 1 {
		t.Fatalf("unexpected centroid amt in c1: want %v, have %v", 1, len(cm2.Centroids))
	}
}

// NOTE: Have this at the bottom of this file for cleanup.
func TestCleanup(t *testing.T) {
	network.stop()
}
