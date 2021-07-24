package rpc

import (
	"net/rpc"
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
