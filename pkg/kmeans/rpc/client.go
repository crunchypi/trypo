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

// Calls the method with the same name on a concrete remote instance of
// common.CentroidManager, using the addr and namespace specified while
// setting up this client with KMeansClient(...).
func (c *kmeansClient) Vec() []float64 {
	var resp []float64

	c.client(func(rc *rpc.Client) {
		*c.err = rc.Call("KMeansServer.Vec", c.namespace, &resp)
	})

	return resp
}

// Calls the method with the same name on a concrete remote instance of
// common.CentroidManager, using the addr and namespace specified while
// setting up this client with KMeansClient(...).
func (c *kmeansClient) AddDataPoint(dp DataPoint) bool {
	var resp bool

	c.client(func(rc *rpc.Client) {
		args := AddDataPointArgs{NameSpace: c.namespace, DP: dp}
		*c.err = rc.Call("KMeansServer.AddDataPoint", args, &resp)
	})

	return resp
}
