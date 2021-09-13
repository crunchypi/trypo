package api

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"testing"
	"time"
	"trypo/core/testutils"
	"trypo/pkg/mathutils"
)

// For the http server.
var apiAddr = Addr{"localhost", "3000"}

// RPC addresses.
var addrs = []Addr{
	{"localhost", "3001"},
	{"localhost", "3002"},
	{"localhost", "3003"},
}

var namespace = "test"
var network testutils.TNetwork = testutils.NewTNetwork(addrs)

// post handles marshalling and posting.
func postData(url string, data interface{}) (*http.Response, error) {
	b, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	r, err := http.Post(url, "application/json", bytes.NewBuffer(b))
	return r, err
}

// Test '/api/dp/put' and '/api/dp/query' endpoints.
func TestDataPoint(t *testing.T) {
	network.Reset()
	defer network.Reset()

	// Start http server.
	go func() {
		err := Start(APIConfig{
			Addr:         apiAddr,
			ReadTimeout:  time.Second * 5,
			WriteTimeout: time.Second * 5,
		})
		t.Log(err)
	}()

	// Give server some time to load.
	time.Sleep(1)

	// Used for putting and querying.
	dp := DP{Vec: []float64{1, 2, 3}, Expires: time.Now().Add(time.Hour)}

	// Put.
	putArgs := struct {
		AddrOptions []string `json:"addressOptions"`
		Namespace   string   `json:"namespace"`
		Accurate    bool     `json:"accurate"`
		DP          DP       `json:"dp"`
	}{
		AddrOptions: addrsToStrs(addrs),
		Namespace:   namespace,
		Accurate:    true,
		DP:          dp,
	}

	if _, err := postData("http://"+apiAddr.ToStr()+"/api/dp/put", putArgs); err != nil {
		t.Fatalf("post err (put): %v", err)
	}

	// Query.
	queryArgs := struct {
		AddrOptions []string  `json:"addressOptions"`
		Namespace   string    `json:"namespace"`
		Accurate    bool      `json:"accurate"`
		QueryVec    []float64 `json:"queryVec"`
		N           int       `json:"n"`
		Drain       bool      `json:"drain"`
	}{
		AddrOptions: addrsToStrs(addrs),
		Namespace:   namespace,
		Accurate:    true,
		QueryVec:    dp.Vec,
		N:           3,
		Drain:       false,
	}

	r, err := postData("http://"+apiAddr.ToStr()+"/api/dp/query", queryArgs)
	if err != nil {
		t.Fatalf("post err (query): %v", err)
	}

	// Check.
	dpResp := make([]DP, 0, 2)
	body, _ := ioutil.ReadAll(r.Body)
	if err := json.Unmarshal(body, &dpResp); err != nil {
		t.Fatalf("unmarshal err: %v", err)
	}

	if len(dpResp) != 1 {
		t.Fatalf("unexpected resp len. want 1, got %v", len(dpResp))
	}

	if !mathutils.VecEq(dp.Vec, dpResp[0].Vec) {
		t.Fatalf("didn't get expected response dp vec")
	}

}

func TestCleanup(t *testing.T) {
	network.Stop()
}
