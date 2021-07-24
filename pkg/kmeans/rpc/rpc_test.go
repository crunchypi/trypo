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

// NOTE: Have this at the bottom of this file for cleanup.
func TestCleanup(t *testing.T) {
	network.stop()
}
