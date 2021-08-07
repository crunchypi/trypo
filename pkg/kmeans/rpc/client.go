/*

Client for RPC layer on top of pkg/kmeans/centroidmanager, mostly (a few extras
are defined here as well). Has most methods with an identical signature -- a few
deviation exist due to limitations in Go (can't send closures or interfaces).

*/
package rpc

import (
	"net/rpc"
	"trypo/pkg/kmeans/centroid"
)

// client tries to connect to a remote client with c.remoteAddr, give
// it to the task func, then clean the client up. It is meant to reduce
// some rpc boilerplate.
func (c *kmeansClient) client(taskF func(*rpc.Client)) {
	client, err := rpc.Dial("tcp", c.remoteAddr)
	if err != nil {
		*c.err = err
		return
	}
	defer client.Close()
	taskF(client)
}

// Namespaces fetches all namespaces stored in remote server.
func (c *kmeansClient) Namespaces() []string {
	var resp []string

	c.client(func(rc *rpc.Client) {
		*c.err = rc.Call("KMeansServer.Namespaces", nil, &resp)
	})

	return resp
}

// Calls the method with the same name on a remote instance of T CentroidManager
// (T of pkg/kmeans/centroidmanager, se that method name for more documentation),
// using the addr and namespace specified while setting up this client.
func (c *kmeansClient) Vec() []float64 {
	var resp []float64

	c.client(func(rc *rpc.Client) {
		*c.err = rc.Call("KMeansServer.Vec", c.namespace, &resp)
	})

	return resp
}

// Calls the method with the same name on a remote instance of T CentroidManager
// (T of pkg/kmeans/centroidmanager, se that method name for more documentation),
// using the addr and namespace specified while setting up this client. Note, will
// create a new CentroidManager instance with the specified namespace if one does
// not already exist.
func (c *kmeansClient) AddDataPoint(dp DataPoint) bool {
	var resp bool

	c.client(func(rc *rpc.Client) {
		args := AddDataPointArgs{NameSpace: c.namespace, DP: dp}
		*c.err = rc.Call("KMeansServer.AddDataPoint", args, &resp)
	})

	return resp
}

// Calls the method with the same name on a remote instance of T CentroidManager
// (T of pkg/kmeans/centroidmanager, se that method name for more documentation),
// using the addr and namespace specified while setting up this client.
func (c *kmeansClient) DrainUnordered(n int) []DataPoint {
	var resp []DataPoint

	c.client(func(rc *rpc.Client) {
		args := DrainArgs{NameSpace: c.namespace, N: n}
		*c.err = rc.Call("KMeansServer.DrainUnordered", args, &resp)
	})

	return resp
}

// Calls the method with the same name on a remote instance of T CentroidManager
// (T of pkg/kmeans/centroidmanager, se that method name for more documentation),
// using the addr and namespace specified while setting up this client.
func (c *kmeansClient) DrainOrdered(n int) []DataPoint {
	var resp []DataPoint

	c.client(func(rc *rpc.Client) {
		args := DrainArgs{NameSpace: c.namespace, N: n}
		*c.err = rc.Call("KMeansServer.DrainOrdered", args, &resp)
	})

	return resp
}

// Calls the method with the same name on a remote instance of T CentroidManager
// (T of pkg/kmeans/centroidmanager, se that method name for more documentation),
// using the addr and namespace specified while setting up this client.
func (c *kmeansClient) Expire() {
	c.client(func(rc *rpc.Client) {
		*c.err = rc.Call("KMeansServer.Expire", c.namespace, nil)
	})
}

// Calls the method with the same name on a remote instance of T CentroidManager
// (T of pkg/kmeans/centroidmanager, se that method name for more documentation),
// using the addr and namespace specified while setting up this client.
func (c *kmeansClient) LenDP() int {
	var resp int

	c.client(func(rc *rpc.Client) {
		*c.err = rc.Call("KMeansServer.LenDP", c.namespace, &resp)
	})

	return resp
}

// Calls the method with the same name on a remote instance of T CentroidManager
// (T of pkg/kmeans/centroidmanager, se that method name for more documentation),
// using the addr and namespace specified while setting up this client.
func (c *kmeansClient) MemTrim() {
	c.client(func(rc *rpc.Client) {
		*c.err = rc.Call("KMeansServer.MemTrim", c.namespace, nil)
	})
}

// Calls the method with the same name on a remote instance of T CentroidManager
// (T of pkg/kmeans/centroidmanager, se that method name for more documentation),
// using the addr and namespace specified while setting up this client.
func (c *kmeansClient) MoveVector() bool {
	var resp bool

	c.client(func(rc *rpc.Client) {
		*c.err = rc.Call("KMeansServer.MoveVector", c.namespace, &resp)
	})

	return resp
}

// DistributeDataPointsFast will try to distribute n datapoints in haste (with
// some accuracy) from _this_ node (specified while setting up this client)
// amongst _other_ 'best-fit' nodes listed in 'addrs'. The CentroidManager
// in _this_ node will have its DrainOrdered method called, then those dps
// will be sent to _other_ nodes that are most similar to the dps (similarity
// is derived from CentroidManager.Vec() on _other_ nodes). Note, all of this
// is done within the same namespace that was used while setting up this client.
func (c *kmeansClient) DistributeDataPointsFast(addrs []string, n int) {
	c.client(func(rc *rpc.Client) {
		args := DistribDPArgs{NameSpace: c.namespace, N: n, AddrOptions: addrs}
		*c.err = rc.Call("KMeansServer.DistributeDataPointsFast", args, nil)
	})
}

// DistributeDataPointsAccurate is similar to DistributeDataPointsFast but is
// slower and more accurate. The latter finds nodes that are most similar to
// the drained datapoints by using KMeansClient(...).Vec() _once_ for each
// address option, while this method uses KMeansClient(...).NearestCentroidVec(..)
// (slower and more accurate method) for each dp and for each address option.
// This is _a_lot_ slower due to many network calls, but has the benefit of
// placing distribute dps precisely.
func (c *kmeansClient) DistributeDataPointsAccurate(addrs []string, n int) {
	c.client(func(rc *rpc.Client) {
		args := DistribDPArgs{NameSpace: c.namespace, N: n, AddrOptions: addrs}
		*c.err = rc.Call("KMeansServer.DistributeDataPointsAccurate", args, nil)
	})
}

// Calls the method with the same name on a remote instance of T CentroidManager
// (T of pkg/kmeans/centroidmanager, se that method name for more documentation),
// using the addr and namespace specified while setting up this client.
func (c *kmeansClient) DistributeDataPointsInternal(n int) {
	c.client(func(rc *rpc.Client) {
		// Line length < 80 ish.
		s := "KMeansServer.DistributeDataPointsInternal"
		args := DistribDPIArgs{NameSpace: c.namespace, N: n}
		*c.err = rc.Call(s, args, nil)
	})
}

// Calls the method with the same name on a remote instance of T CentroidManager
// (T of pkg/kmeans/centroidmanager, se that method name for more documentation),
// using the addr and namespace specified while setting up this client.
func (c *kmeansClient) KNNLookup(vec []float64, k int, drain bool) []DataPoint {
	resp := make([]DataPoint, 0, k)

	c.client(func(rc *rpc.Client) {
		args := KNNLookupArgs{NameSpace: c.namespace, Vec: vec, K: k, Drain: drain}
		*c.err = rc.Call("KMeansServer.KNNLookup", args, &resp)
	})

	return resp
}

// Calls the method with the same name on a remote instance of T CentroidManager
// (T of pkg/kmeans/centroidmanager, se that method name for more documentation),
// using the addr and namespace specified while setting up this client.
//
// Note; centroids that are sent over the network need to be re-initialised.
// This is done using the CentroidFactory field of this kmeansClient instance,
// so that has to be configured (a default exists, configured with cosine
// similarity/distance funcs, used in methods such as DrainUnordered).
// Additionally, empty centroids are filtered out.
func (c *kmeansClient) NearestCentroids(vec []float64, n int, drain bool) (
	[]*centroid.Centroid, bool,
) {
	var resp []*Centroid

	c.client(func(rc *rpc.Client) {
		args := NearestCentroidArgs{NameSpace: c.namespace, Vec: vec, N: n, Drain: drain}
		*c.err = rc.Call("KMeansServer.NearestCentroids", args, &resp)
	})

	if resp == nil || len(resp) == 0 {
		return nil, false
	}

	// Remove nil/empty(no dps) centroids.
	for i := 0; i < len(resp); i++ {
		if resp[i] == nil || resp[i].LenDP() == 0 {
			resp = append(resp[:i], resp[i+1:]...)
		}
	}

	// At the time of writing, the returned Centroids are partially broken.
	// The kmeans/Centroid.Centroid type has some unexported fields, so they
	// become nil (such as vec and knnSearchFunc), and that will cause a
	// nil pointer deref. To prevent these issues, the centroids are re-init.
	for i := 0; i < len(resp); i++ {
		dps := resp[i].DataPoints
		// No empty check since empty Centroids were removed above.
		newCentroid := c.CentroidFactory(dps[0].Vec)
		newCentroid.DataPoints = dps
		newCentroid.MoveVector()
		resp[i] = newCentroid
	}

	return resp, true
}

// Connects to a remmote node using the addr and namespace specified while
// setting up this client, and finds a Centroid that is nearest 'vec' before
// returning the vector of that Centroid. 'nearest' will depend on how the
// remote Centroid was set up.
func (c *kmeansClient) NearestCentroidVec(vec []float64) []float64 {
	var resp []float64

	c.client(func(rc *rpc.Client) {
		args := NearestCentroidVecArgs{NameSpace: c.namespace, Vec: vec}
		*c.err = rc.Call("KMeansServer.NearestCentroidVec", args, &resp)
	})

	return resp
}

// Calls the method with the same name on a remote instance of T CentroidManager
// (T of pkg/kmeans/centroidmanager, se that method name for more documentation),
// using the addr and namespace specified while setting up this client.
//
// Note: the method signature is different than with CentroidManager, which
// accepts 'func(*Centroid) bool' as arg, because RPC in go can't send funcs.
// Instead, a range can be specified here such that remote Centroids are split
// if their contained amt of DPs falls within the range (min&max are _exclusive_).
func (c *kmeansClient) SplitCentroids(dpRangeMin, dpRangeMax int) {
	c.client(func(rc *rpc.Client) {
		args := SplitCentroidsArgs{
			NameSpace:  c.namespace,
			DPRangeMin: dpRangeMin,
			DPRangeMax: dpRangeMax,
		}
		*c.err = rc.Call("KMeansServer.SplitCentroids", args, nil)
	})
}

// Calls the method with the same name on a remote instance of T CentroidManager
// (T of pkg/kmeans/centroidmanager, se that method name for more documentation),
// using the addr and namespace specified while setting up this client.
//
// Note: the method signature is different than with CentroidManager, which
// accepts 'func(*Centroid) bool' as arg, because RPC in go can't send funcs.
// Instead, a range can be specified here such that remote Centroids are merged
// if their contained amt of DPs falls within the range (min&max are _exclusive_).
func (c *kmeansClient) MergeCentroids(dpRangeMin, dpRangeMax int) {
	c.client(func(rc *rpc.Client) {
		args := SplitCentroidsArgs{
			NameSpace:  c.namespace,
			DPRangeMin: dpRangeMin,
			DPRangeMax: dpRangeMax,
		}
		*c.err = rc.Call("KMeansServer.MergeCentroids", args, nil)
	})
}

// StealCentroids will 'steal' one or more Centroid from a remote node, intended for
// load balancing. If A=(the node contacted with this method) and B=(the node which
// A steals from, with addr 'fromAddr'), then A will keep 'stealing' _whole_
// Centroids from B until the total amount of transferred datapoints exceeds
// 'transferLimit' (this value might therefore be greatly overshot), using
// KMeansClient(...).NearestCentroid(vec), where vec is the vector of CentroidManager
// in A with the supplied namespace. The response is a bool and an int, where the
// former represents total amount of datapoints transferred, while the latter
// indicates whether or not there was a network/namespace issue between A & B.
// The configuration of the int and bool have these implied meanings:
//	- int = 0 & bool = false : remote node err (namespace or network issue).
//	- int > 0 & bool = false : Some Centroids transferred before network err.
//	- int = 0 & bool = true : No network err but remote is empty.
//	- int > 0 & bool = true : all ok.
func (c *kmeansClient) StealCentroids(fromAddr string, transferLimit int) (int, bool) {
	var n int
	var ok bool
	c.client(func(rc *rpc.Client) {
		args := StealCentroidArgs{
			FromAddr:        fromAddr,
			NameSpace:       c.namespace,
			TransferDPLimit: transferLimit,
		}
		resp := StealCentroidsResp{}
		*c.err = rc.Call("KMeansServer.StealCentroid", args, &resp)
		n = resp.TransferredN
		ok = resp.OK
	})
	return n, ok
}
