/*

Client for RPC layer on top of pkg/kmeans/centroidmanager. Has most methods
with an identical signature -- a few deviation exist due to limitations in
Go (can't send closures or interfaces).

*/
package rpc

import (
	"net/rpc"
	"trypo/pkg/kmeans/centroid"
	"trypo/pkg/kmeans/common"
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

// DistributeDataPoints works with a similar intent as the method with the same name
// on pkg/kmeans/centroidmanager.CentroidManager; it gets a slice of DataPoint from
// a remote node (addr specified while setting up this client, calling DrainOrdered),
// then adds them to 'best-fit' 'receivers'. 'best-fit' is calculated with a
// similarity function that has to be configured with the KNNSearchFunc field on
// this kmeansClient (a default exists, configured with cosine similarity).
func (c *kmeansClient) DistributeDataPoints(n int, receivers []common.DataPointReceiver) {
	if receivers == nil || len(receivers) == 0 {
		return
	}
	// Drain data from distributer.
	data := c.DrainOrdered(n)

	// Get vectors from receivers. It is assumed here that receivers are actually
	// concrete kmeansClient instances, and their vecs are gathered here. This is
	// because these vectors will potentially be fetched multiple times (see
	// block after 'generator' definition, where the loop resets the generator)
	// so it's probably a good idea to do caching for minimal network activity.
	rcvVecs := make([][]float64, len(receivers))

	// index correlation between 'receivers' and rcvVecs for convenience.
	type rcvResp struct {
		vec []float64
		i   int
	}

	ch := make(chan rcvResp, len(receivers))
	// Fetch.
	for i := 0; i < len(receivers); i++ {
		go func(i int) {
			ch <- rcvResp{vec: receivers[i].Vec(), i: i}
		}(i)
	}
	// Collect.
	for i := 0; i < len(receivers); i++ {
		resp := <-ch
		rcvVecs[resp.i] = resp.vec
	}

	// The generator, it might reset multiple times, which
	// is the reason for fetching the vecs above.
	i := 0
	generator := func() ([]float64, bool) {
		if i >= len(rcvVecs) {
			return nil, false
		}
		i++
		return rcvVecs[i-1], true
	}

	c.client(func(rc *rpc.Client) {
		for j := 0; j < len(data); j++ {
			i = 0 // Reset generator.
			indexes := c.KNNSearchFunc(data[j].Vec, generator, 1)
			// Put back into self if (1) search failed or (2) adder failed to add.
			if len(indexes) == 0 || !receivers[indexes[0]].AddDataPoint(data[j]) {
				c.AddDataPoint(data[j])
			}
		}
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
