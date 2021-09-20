package api

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"trypo/core/dps"
	"trypo/pkg/kmeans/common"
	"trypo/pkg/searchutils"
)

type handler struct {
	// RPCAddrs should contain all addresses used in RPC network which contains
	// all the nodes which handle the data used in this system and service (i.e
	// approximate nearest neighs search). Should contain addr for the local rpc
	// instance, not to be confused with the Addr (port) used for the API.
	RPCAddrs []Addr
}

func (h *handler) setRoutes() {
	routes := map[string]func(http.ResponseWriter, *http.Request){
		"/api/dp/put":   h.putDataPoint,
		"/api/dp/query": h.queryDataPoint,
	}
	for k, v := range routes {
		http.Handle(k, http.HandlerFunc(v))
		fmt.Printf("route '%v' is up.\n", k)
	}
}

// tryUnpackRequestOptions will try to unmarshal the request body into
// <targetOpt>. If the task fails, then an automatic bad request
// response is sent to the requester and false is returned. Else,
// nothing is written to the requester and the return is true.
func (h *handler) tryUnpackRequestOptions(
	w http.ResponseWriter, r *http.Request, targetOpt interface{}) bool {
	// Error is not necessary to check, if it's not nil then
	// the body with JSON request isn't going to work anyway.
	body, _ := ioutil.ReadAll(r.Body)
	err := json.Unmarshal(body, targetOpt)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return false
	}
	return true
}

// Pass request to dps.PutDataPointX (core/dps/putdps.go).
func (h *handler) putDataPoint(w http.ResponseWriter, r *http.Request) {
	opts := struct {
		Namespace string `json:"namespace"`
		Accurate  bool   `json:"accurate"`
		DP        DP     `json:"dp"`
	}{}

	// opts unpack.
	if !h.tryUnpackRequestOptions(w, r, &opts) {
		return
	}

	// pass to dps pkg.
	args := dps.PutDataPointArgs{
		AddrOptions:   h.RPCAddrs,
		Namespace:     opts.Namespace,
		DataPoint:     opts.DP.toDataPoint(),
		KNNSearchFunc: searchutils.KNNCos,
	}

	putOk := false
	switch opts.Accurate {
	case true:
		putOk = dps.PutDataPointAccurate(args)
	case false:
		putOk = dps.PutDataPointFast(args)
	}

	// Will be false if none of the nodes are initialised.
	if !putOk {
		putOk = dps.PutDataPointRand(args)
	}

	// reply.
	switch putOk {
	case true:
		w.WriteHeader(http.StatusOK)
	case false:
		w.WriteHeader(http.StatusInternalServerError)
	}
}

// Pass request to dps.GetDataPointX (core/dps/getdps.go).
func (h *handler) queryDataPoint(w http.ResponseWriter, r *http.Request) {
	opts := struct {
		Namespace string    `json:"namespace"`
		Accurate  bool      `json:"accurate"`
		QueryVec  []float64 `json:"queryVec"`
		N         int       `json:"n"`
		Drain     bool      `json:"drain"`
	}{}

	// opts unpack.
	if !h.tryUnpackRequestOptions(w, r, &opts) {
		return
	}

	// pass to dps pkg.
	args := dps.GetDataPointsArgs{
		AddrOptions:   h.RPCAddrs,
		Namespace:     opts.Namespace,
		QueryVec:      opts.QueryVec,
		N:             opts.N,
		Drain:         opts.Drain,
		KNNSearchFunc: searchutils.KNNCos,
	}

	var resp []common.DataPoint
	switch opts.Accurate {
	case true:
		resp = dps.GetDataPointsAccurate(args)
	case false:
		resp = dps.GetDataPointsFast(args)
	}

	// reply.
	var respConv []DP
	if resp != nil {
		respConv = DataPointsToDPs(resp)
	}

	b, _ := json.Marshal(respConv)
	w.WriteHeader(http.StatusOK)
	w.Write(b)
}
