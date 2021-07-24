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
	defer network.reset()
	namespace := "test"
	addr := addrs[0]

	vec1 := vec(3, 4)

	slot := CManagerSlot{cManager: newCentroidManager(vec1)}
	network.nodes[addr].Table.slots[namespace] = &slot

	var err error
	vec2 := KMeansClient(addr, namespace, &err).Vec()
	if err != nil {
		t.Fatalf("client conn (?) err: %v", err)
	}

	if !vecEq(vec1, vec2) {
		t.Fatalf("got incorrect vector. want %v, got %v", vec1, vec2)
	}
}

func TestAddDataPoint(t *testing.T) {
	if !t.Run("DEPENDENCY 1", TestVec) {
		t.Fatalf("expected TestVec to work, it did not")
	}

	defer network.reset()
	namespace := "test"
	addr := addrs[0]
	vec1 := vec(1, 5)

	var err error
	// ok bool ignored because there are currently no CentroidManager
	// instances in any server, and this method adds a new CentroidManager
	// if no namespaces expist/are valid.
	KMeansClient(addr, namespace, &err).AddDataPoint(dp(vec1, 0))

	if err != nil {
		t.Fatalf("client conn (?) err: %v", err)
	}

	if !vecEq(KMeansClient(addr, namespace, nil).Vec(), vec1) {
		t.Fatalf("remote vector incorrect")
	}
}

// NOTE: Have this at the bottom of this file for cleanup.
func TestCleanup(t *testing.T) {
	network.stop()
}
