package rpc

import (
	"net/rpc"
)

// Calls the method with the same name on a concrete remote instance of
// common.CentroidManager, using the addr and namespace specified while
// setting up this client with KMeansClient(...).
func (c *kmeansClient) Vec() []float64 {
	client, err := rpc.Dial("tcp", c.remoteAddr)
	if err != nil {
		*c.err = err
		return nil
	}
	defer client.Close()

	var resp []float64
	*c.err = client.Call("KMeansServer.Vec", c.namespace, &resp)
	return resp
}

// Calls the method with the same name on a concrete remote instance of
// common.CentroidManager, using the addr and namespace specified while
// setting up this client with KMeansClient(...).
func (c *kmeansClient) AddDataPoint(dp DataPoint) bool {
	client, err := rpc.Dial("tcp", c.remoteAddr)
	if err != nil {
		*c.err = err
		return false
	}
	defer client.Close()

	args := AddDataPointArgs{NameSpace: c.namespace, DP: dp}
	var resp bool
	*c.err = client.Call("KMeansServer.AddDataPoint", args, &resp)
	return resp
}
