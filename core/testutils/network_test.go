package testutils

import (
	"testing"
	"time"
	"trypo/pkg/arbiter"
	"trypo/pkg/kmeans/common"
	"trypo/pkg/kmeans/rpc"
	"trypo/pkg/mathutils"
)

var addrs = []Addr{
	{"localhost", "3000"},
	{"localhost", "3001"},
	{"localhost", "3002"},
}
var namespace = "test"
var network = NewTNetwork(addrs)

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

// Pretty simple test: find network consensus & add a datapoint.
func TestStart(t *testing.T) {
	defer network.Reset()

	tries := 1000
	errs := make(arbiter.ArbErrs)
	ok := arbiter.ArbiterClients(addrs, errs, nil).TryForceNewArbiter(tries)

	if _, err := errs.CheckAll(); err != nil {
		t.Fatalf("TryForce err: %v", err)
	}

	if !ok {
		t.Fatalf("failed consensus after %v tries", tries)
	}

	dp := dp(vec(1, 2), 0)
	addr := addrs[0]

	var err error
	client := rpc.KMeansClient(addr.ToStr(), namespace, &err)
	client.AddDataPoint(dp)

	if err != nil {
		t.Fatal(err)
	}

	km := network.UnwrapCM(addr, namespace)
	if km == nil {
		t.Fatal("nil km")
	}
	if km.LenDP() != 1 {
		t.Fatal("unexpected km dps len")
	}
	dpRemote := km.Centroids[0].DataPoints[0]
	if !vecEq(dpRemote.Vec, dp.Vec) {
		t.Fatalf("wrong dp vec. want %v, have %v", dp.Vec, dpRemote.Vec)
	}
}

func TestCleanup(t *testing.T) {
	network.Stop()
}
