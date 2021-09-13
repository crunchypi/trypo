package nodes

import (
	"testing"
	"time"
	"trypo/core/testutils"
	"trypo/pkg/kmeans/centroid"
	"trypo/pkg/kmeans/common"
	"trypo/pkg/kmeans/rpc"
	"trypo/pkg/mathutils"
	"trypo/pkg/searchutils"
)

var addrs = []Addr{
	{"localhost", "3000"},
	{"localhost", "3001"},
	{"localhost", "3002"},
}
var namespace = "test"
var network = testutils.NewTNetwork(addrs)

var vec = mathutils.Vec     // Create new vec.
var vecEq = mathutils.VecEq // compare two vecs.
var vecIn = mathutils.VecIn // Membership check.

func dp(v []float64, sleepUnits int) common.DataPoint {
	_dp := common.DataPoint{Vec: v}

	if sleepUnits > 0 {
		_dp.Expires = time.Now().Add(time.Duration(sleepUnits))
		_dp.ExpireEnabled = true
	}
	return _dp
}

func TestFetchVecsFast(t *testing.T) {
	network.Reset()
	defer network.Reset()

	vec1 := vec(1, 1)
	vec2 := vec(1, 2)
	vec3 := vec(1, 3)

	addrsVecs := map[Addr][]float64{
		addrs[0]: vec1,
		addrs[1]: vec2,
		addrs[2]: vec3,
	}

	// Doing a AddDataPoint call will instantiate a remote CentroidManager.
	for addr, vec := range addrsVecs {
		client := rpc.KMeansClient(addr.ToStr(), namespace, nil)
		if !client.AddDataPoint(dp(vec, 0)) {
			t.Fatalf("unexpected 'not ok' for %v", addr.ToStr())
		}
	}

	// Confirm added.
	for addr, vec := range addrsVecs {
		cm := network.UnwrapCM(addr, namespace)
		if !vecEq(vec, cm.Vec()) {
			s := "unexpected cm vec for %v. want %v, got %v"
			t.Fatalf(s, addr.ToStr(), vec, cm.Vec())
		}
	}

	// Confirm vecs (weak validation).
	fch := fetchVecsFast(addrs, namespace)
	for _, res := range *fch.collect() {
		if !vecIn(res.vec, [][]float64{vec1, vec2, vec3}) {
			t.Fatalf("unknown res vec: %v", res.vec)
		}
	}
}

func TestFetchVecsAccurate(t *testing.T) {
	network.Reset()
	defer network.Reset()

	addrsVecs := map[Addr][]float64{
		addrs[0]: vec(1, 1),
		addrs[1]: vec(1, 1),
		addrs[2]: vec(1, 1),
	}

	// Doing a AddDataPoint call will instantiate a remote CentroidManager.
	for addr, vec := range addrsVecs {
		client := rpc.KMeansClient(addr.ToStr(), namespace, nil)
		if !client.AddDataPoint(dp(vec, 0)) {
			t.Fatalf("unexpected 'not ok' for %v", addr.ToStr())
		}
	}

	// fetchVecsAccurate (the thing that is tested here) finds the most
	// accurate vec on a pkg/kmeans/centroid.Centroid level, as opposed
	// to pkg/kmeans/centroidmanager.CentroidManager level. So to test
	// the stuff, actual Centroids should be created carefully.
	targetVec := vec(1, 4)
	network.UnwrapCM(addrs[0], namespace).Centroids = []*centroid.Centroid{
		testutils.NewCentroid(vec(1, 1)),
		testutils.NewCentroid(vec(1, 2)),
	}
	network.UnwrapCM(addrs[1], namespace).Centroids = []*centroid.Centroid{
		testutils.NewCentroid(vec(1, 3)),
		testutils.NewCentroid(targetVec), // <- best match.
	}
	network.UnwrapCM(addrs[2], namespace).Centroids = []*centroid.Centroid{
		testutils.NewCentroid(vec(1, 5)),
		testutils.NewCentroid(vec(1, 6)),
	}

	// Check multiple times; an incorrect implementation will give an
	// order (of the 'addr' variable) that depends on network speed etc.
	for i := 0; i < 10; i++ {
		fch := fetchVecsAccurate(addrs, namespace, targetVec)
		res := *fch.collect()

		vecs := [][]float64{res[0].vec, res[1].vec, res[2].vec}

		if !vecIn(targetVec, vecs) {
			s := "'target vec' (%v) not in vecs (%v)"
			t.Fatalf(s, targetVec, vecs)

		}
	}
}

func TestBestFitNodesFast(t *testing.T) {
	network.Reset()
	defer network.Reset()

	vec1 := vec(1, 1)
	vec2 := vec(1, 2)
	vec3 := vec(1, 3)

	addrsVecs := map[Addr][]float64{
		addrs[0]: vec1,
		addrs[1]: vec2,
		addrs[2]: vec3,
	}

	// Add remote vecs.
	for addr, vec := range addrsVecs {
		client := rpc.KMeansClient(addr.ToStr(), namespace, nil)
		if !client.AddDataPoint(dp(vec, 0)) {
			t.Fatalf("unexpected 'not ok' for %v", addr.ToStr())
		}
	}

	// Confirm added.
	for addr, vec := range addrsVecs {
		cm := network.UnwrapCM(addr, namespace)
		if !vecEq(vec, cm.Vec()) {
			s := "unexpected cm vec for %v. want %v, got %v"
			t.Fatalf(s, addr.ToStr(), vec, cm.Vec())
		}
	}

	// Check multiple times; an incorrect implementation will give an
	// order (of the 'addr' variable) that depends on network speed etc.
	for i := 0; i < 10; i++ {

		// Using vec3 as Vec makes the order: vec3, vec2, vec1.
		// in similarity priority.
		rAddrs := BestFitNodesFast(BestFitNodesArgs{
			AddrOpts:      addrs,
			Namespace:     namespace,
			Vec:           vec3,
			KNNSearchFunc: searchutils.KNNCos,
		})

		if !rAddrs[0].Comp(addrs[2]) {
			s := "incorrect order (1). want %v, got %v"
			t.Fatal(s, rAddrs[0], addrs[2])
		}
		if !rAddrs[1].Comp(addrs[1]) {
			s := "incorrect order (1). want %v, got %v"
			t.Fatal(s, rAddrs[1], addrs[1])
		}
		if !rAddrs[2].Comp(addrs[0]) {
			s := "incorrect order (1). want %v, got %v"
			t.Fatal(s, rAddrs[2], addrs[0])
		}
	}
}

func TestBestFitNodesAccurate(t *testing.T) {
	network.Reset()
	defer network.Reset()

	addrsVecs := map[Addr][]float64{
		addrs[0]: vec(1, 1),
		addrs[1]: vec(1, 1),
		addrs[2]: vec(1, 1),
	}

	// Doing a AddDataPoint call will instantiate a remote CentroidManager.
	for addr, vec := range addrsVecs {
		client := rpc.KMeansClient(addr.ToStr(), namespace, nil)
		if !client.AddDataPoint(dp(vec, 0)) {
			t.Fatalf("unexpected 'not ok' for %v", addr.ToStr())
		}
	}

	targetVec := vec(1, 4)
	network.UnwrapCM(addrs[0], namespace).Centroids = []*centroid.Centroid{
		testutils.NewCentroid(vec(1, 1)),
		testutils.NewCentroid(vec(1, 2)),
	}
	network.UnwrapCM(addrs[1], namespace).Centroids = []*centroid.Centroid{
		testutils.NewCentroid(vec(1, 3)),
		testutils.NewCentroid(targetVec), // <- best match.
	}
	network.UnwrapCM(addrs[2], namespace).Centroids = []*centroid.Centroid{
		testutils.NewCentroid(vec(1, 5)),
		testutils.NewCentroid(vec(1, 6)),
	}

	// Check multiple times; an incorrect implementation will give an
	// order (of the 'addr' variable) that depends on network speed etc.
	for i := 0; i < 10; i++ {
		rAddrs := BestFitNodesAccurate(BestFitNodesArgs{
			AddrOpts:      addrs,
			Namespace:     namespace,
			Vec:           targetVec,
			KNNSearchFunc: searchutils.KNNCos,
		})

		if !rAddrs[0].Comp(addrs[1]) {
			s := "unexpected best addr. Want %v, got %v"
			t.Fatalf(s, addrs[1], rAddrs[0])
		}
	}
}

func TestCleanup(t *testing.T) {
	defer network.Stop()
}
