package dps

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

func dp(v []float64, sleepUnits int) common.DataPoint {
	_dp := common.DataPoint{Vec: v}

	if sleepUnits > 0 {
		_dp.Expires = time.Now().Add(time.Duration(sleepUnits))
		_dp.ExpireEnabled = true
	}
	return _dp
}

func TestGetDataPointsRand(t *testing.T) {
	network.Reset()
	defer network.Reset()

	for _, addr := range addrs {
		dp := DataPoint{}
		var err error
		ok := rpc.KMeansClient(addr.ToStr(), namespace, &err).AddDataPoint(dp)

		if err != nil {
			t.Fatalf("network err for %v: %v", addr.ToStr(), err)
		}
		if !ok {
			t.Fatalf("unexpected 'not ok' for %v", addr.ToStr())
		}
	}

	dps := GetDataPointsRand(GetDataPointsArgs{
		AddrOptions: addrs,
		Namespace:   namespace,
		N:           1,
		Drain:       true,
	})

	if len(dps) != 1 {
		t.Fatalf("unexpected dps len: %v", len(dps))
	}

	dpLen := 0
	for _, addr := range addrs {
		km := network.UnwrapCM(addr, namespace)
		dpLen += km.LenDP()
	}

	if dpLen != 2 {
		t.Fatal("none got drained")
	}
}

func TestGetDataPointsFast(t *testing.T) {
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

	dps := GetDataPointsFast(GetDataPointsArgs{
		AddrOptions:   addrs,
		Namespace:     namespace,
		QueryVec:      vec2,
		N:             1,
		Drain:         true,
		KNNSearchFunc: searchutils.KNNCos,
	})

	if len(dps) != 1 {
		t.Fatalf("unexpected dps len: %v", len(dps))
	}
	if !vecEq(dps[0].Vec, vec2) {
		t.Fatalf("unexpected dp resp with vec %v", dps[0].Vec)
	}
	if rpc.KMeansClient(addrs[1].ToStr(), namespace, nil).LenDP() != 0 {
		t.Fatalf("remote didn't drain")
	}
}

func TestGetDataPointsAccurate(t *testing.T) {
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

	c1 := testutils.NewCentroid(vec(1, 1))
	c1.AddDataPoint(dp(vec(1, 1), 0))

	c2 := testutils.NewCentroid(vec(1, 2))
	c2.AddDataPoint(dp(vec(1, 2), 0))

	c3 := testutils.NewCentroid(vec(1, 3))
	c3.AddDataPoint(dp(vec(1, 3), 0))

	c4 := testutils.NewCentroid(targetVec) // <- best match
	c4.AddDataPoint(dp(targetVec, 0))      // <- best match

	c5 := testutils.NewCentroid(vec(1, 5))
	c5.AddDataPoint(dp(vec(1, 5), 0))

	c6 := testutils.NewCentroid(vec(1, 6))
	c6.AddDataPoint(dp(vec(1, 6), 0))

	network.UnwrapCM(addrs[0], namespace).Centroids = []*centroid.Centroid{
		c1, c2,
	}
	network.UnwrapCM(addrs[1], namespace).Centroids = []*centroid.Centroid{
		c3, c4,
	}
	network.UnwrapCM(addrs[2], namespace).Centroids = []*centroid.Centroid{
		c5, c6,
	}

	dps := GetDataPointsAccurate(GetDataPointsArgs{
		AddrOptions:   addrs,
		Namespace:     namespace,
		QueryVec:      targetVec,
		N:             1,
		Drain:         true,
		KNNSearchFunc: searchutils.KNNCos,
	})

	if len(dps) != 1 {
		t.Fatalf("unexpected dps len: %v", len(dps))
	}
	if !vecEq(dps[0].Vec, targetVec) {
		t.Fatalf("unexpected dp resp with vec %v", dps[0].Vec)
	}
	if rpc.KMeansClient(addrs[1].ToStr(), namespace, nil).LenDP() != 1 {
		t.Fatalf("remote didn't drain")
	}
}

func TestPutDataPointsFast(t *testing.T) {
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

	ok := PutDataPointFast(PutDataPointArgs{
		AddrOptions:   addrs,
		Namespace:     namespace,
		DataPoint:     dp(vec2, 0), // Add to addrs[1]
		KNNSearchFunc: searchutils.KNNCos,
	})

	if !ok {
		t.Fatal("didn't add dp (got 'not ok')")
	}

	if rpc.KMeansClient(addrs[1].ToStr(), namespace, nil).LenDP() != 2 {
		t.Fatalf("remote didn't get the dp")
	}
}

func TestPutDataPointsAccurate(t *testing.T) {
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

	c1 := testutils.NewCentroid(vec(1, 1))
	c1.AddDataPoint(dp(vec(1, 1), 0))

	c2 := testutils.NewCentroid(vec(1, 2))
	c2.AddDataPoint(dp(vec(1, 2), 0))

	c3 := testutils.NewCentroid(vec(1, 3))
	c3.AddDataPoint(dp(vec(1, 3), 0))

	c4 := testutils.NewCentroid(targetVec) // <- best match
	c4.AddDataPoint(dp(targetVec, 0))      // <- best match

	c5 := testutils.NewCentroid(vec(1, 5))
	c5.AddDataPoint(dp(vec(1, 5), 0))

	c6 := testutils.NewCentroid(vec(1, 6))
	c6.AddDataPoint(dp(vec(1, 6), 0))

	network.UnwrapCM(addrs[0], namespace).Centroids = []*centroid.Centroid{
		c1, c2,
	}
	network.UnwrapCM(addrs[1], namespace).Centroids = []*centroid.Centroid{
		c3, c4,
	}
	network.UnwrapCM(addrs[2], namespace).Centroids = []*centroid.Centroid{
		c5, c6,
	}

	ok := PutDataPointAccurate(PutDataPointArgs{
		AddrOptions:   addrs,
		Namespace:     namespace,
		DataPoint:     dp(targetVec, 0), // Add to addrs[1]
		KNNSearchFunc: searchutils.KNNCos,
	})

	if !ok {
		t.Fatal("didn't add dp (got 'not ok')")
	}

	if rpc.KMeansClient(addrs[1].ToStr(), namespace, nil).LenDP() != 3 {
		t.Fatalf("remote didn't get the dp")
	}
}

func TestCleanup(t *testing.T) {
	network.Stop()
}
