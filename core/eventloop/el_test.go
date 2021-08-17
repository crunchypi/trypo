/*
This file is more like a monitoring thing than unit tests.
*/
package eventloop

import (
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"
	"trypo/core/dps"
	"trypo/core/testutils"
	"trypo/pkg/kmeans/common"
	"trypo/pkg/kmeans/rpc"
	"trypo/pkg/mathutils"
	"trypo/pkg/searchutils"
)

/*
--------------------------------------------------------------------------------
Some global vars.
--------------------------------------------------------------------------------
*/

var g_addrs = []Addr{
	{"localhost", "3000"},
	{"localhost", "3001"},
	{"localhost", "3002"},
	{"localhost", "3003"},
	{"localhost", "3004"},
	{"localhost", "3005"},
	{"localhost", "3006"},
	{"localhost", "3007"},
	{"localhost", "3008"},
	{"localhost", "3009"},
	//{"localhost", "3010"},
	//{"localhost", "3011"},
	//{"localhost", "3012"},
	//{"localhost", "3013"},
	//{"localhost", "3014"},
	//{"localhost", "3015"},
	//{"localhost", "3016"},
	//{"localhost", "3017"},
	//{"localhost", "3018"},
	//{"localhost", "3019"},
	//{"localhost", "3020"},
	//{"localhost", "3021"},
	//{"localhost", "3022"},
	//{"localhost", "3023"},
	//{"localhost", "3024"},
	//{"localhost", "3025"},
}

var g_namespace = "test"
var g_network = testutils.NewTNetwork(g_addrs)

// How long the monitoring will last.
var g_testSeconds = 60 * 9 // Test timeout panic at 10m.

// DataPoint details (for standardising).
var g_dpDim = 30                         // dp dimension.
var g_dpVecMin = 0.1                     // min vec value.
var g_dpVecMax = 1.0                     // max vec value.
var g_dpExpireSecMin = g_testSeconds / 9 // min dp expiration after creation.
var g_dpExpireSecMax = g_testSeconds     // max dp expiration after creation.

// g_bias=true will only add dps to g_addrs[0] (for the purpose of checking
// how the load-balancing mechanism works). false will distribute new dps
// among all g_addrs randomly.

// g_bias=false will distribute new dps randomly among all addrs in g_addrs,
// true will make it so that there is a bias so 50% of them are added to
// g_addrs[0] (for the purpose of checking how load-balancing works).
var g_bias = true

/*
--------------------------------------------------------------------------------
Some datapoint tools.
--------------------------------------------------------------------------------
*/

func dp(v []float64, expireSeconds int) common.DataPoint {
	_dp := common.DataPoint{Vec: v}

	if expireSeconds > 0 {
		expires := time.Second * time.Duration(expireSeconds)
		_dp.Expires = time.Now().Add(expires)
		_dp.ExpireEnabled = true
	}
	return _dp
}

func dpRand(dim int, vMin, vMax float64, expMin, expMax int) common.DataPoint {
	vec := make([]float64, dim)
	for j := 0; j < dim; j++ {
		rand.Seed(time.Now().UnixNano())
		vec[j] = rand.Float64() * (vMax - vMin)
	}
	rand.Seed(time.Now().UnixNano())
	expires := rand.Intn(expMax) + expMin
	return dp(vec, expires)
}

func dpsRand(n, dim int, vMin, vMax float64, expMin, expMax int) []common.DataPoint {
	r := make([]common.DataPoint, n)

	for i := 0; i < n; i++ {
		r[i] = dpRand(dim, vMin, vMax, expMin, expMax)
	}

	return r
}

// Add a dp randomly to one addr in addrs.
func dpAddRand(addrs []Addr, namespace string, dp common.DataPoint) error {
	rand.Seed(time.Now().UnixNano())

	addr := addrs[rand.Intn(len(addrs))]
	if g_bias && rand.Intn(2) == 1 {
		addr = addrs[0]
	}

	var err error
	rpc.KMeansClient(addr.ToStr(), namespace, &err).AddDataPoint(dp)
	return err
}

/*
--------------------------------------------------------------------------------
Monitor stuff.
--------------------------------------------------------------------------------
*/

type tMonitor struct {
	addrs     []Addr
	namespace string
	network   testutils.TNetwork

	sync.Mutex
	metaData map[Addr]MetaData
	taskData map[Addr]string

	// Accuracy stuff. There are two way of querying the network;
	// in haste and with accuracy. See pollAccuracy method for
	// details about how that is done.
	accuracyN             int
	accuracyTotalFast     float64
	accuracyTotalAccurate float64
}

// Basically just sets the accuracyN, accuracyTotalFast and accuracyTotalAccurate
// fields in tMonitor.
func (m *tMonitor) pollAccuracy() {
	rand.Seed(time.Now().UnixNano())

	// Just get a random dp in the network.
	randVec := dpRand(g_dpDim, g_dpVecMin, g_dpVecMax, 99, 99).Vec

	dpRand := dps.GetDataPointsRand(dps.GetDataPointsArgs{
		AddrOptions:   m.addrs,
		Namespace:     g_namespace,
		QueryVec:      randVec,
		N:             1,
		Drain:         false,
		KNNSearchFunc: searchutils.KNNCos,
	})
	if len(dpRand) == 0 {
		return
	}

	// The code below uses the vec from the random dp to measure
	// accuracy (the query result should be as close to this vec
	// as possible).

	queryVec := dpRand[0].Vec

	args := dps.GetDataPointsArgs{
		AddrOptions:   m.addrs,
		Namespace:     g_namespace,
		QueryVec:      queryVec,
		N:             1,
		Drain:         false,
		KNNSearchFunc: searchutils.KNNCos,
	}

	dpsFast := dps.GetDataPointsFast(args)
	dpsAccurate := dps.GetDataPointsAccurate(args)

	// Disabled/commented out panic calls because they're triggered
	// even though there are not network issues (like 99% sure), the
	// assumed reason is timeouts due to heavy network loads.

	if len(dpsFast) == 0 || len(dpsAccurate) == 0 {
		//panic("failed to poll accuracy")
		return
	}

	fastScore, err := mathutils.CosineSimilarity(queryVec, dpsFast[0].Vec)
	if err != nil {
		//panic("nil score for fast accuracy")
		return
	}

	accurateScore, err := mathutils.CosineSimilarity(queryVec, dpsAccurate[0].Vec)
	if err != nil {
		//panic("nil score for fast accuracy")
		return
	}

	m.accuracyTotalFast += fastScore
	m.accuracyTotalAccurate += accurateScore
	m.accuracyN++
}

// Printout of monitor stuff. Kinda similar to defaultLogger.LogTask in ./log.go
// but puts the task description besides all nodes.
func (m *tMonitor) Refresh() {
	m.Lock()
	defer m.Unlock()

	fmt.Println()
	cmd := exec.Command("clear")
	cmd.Stdout = os.Stdout
	cmd.Run()

	totalDps := 0

	for _, addr := range m.addrs {
		task := m.taskData[addr]
		meta := m.metaData[addr].Items[addr]

		dpLen := 0
		for _, l := range meta.LenDP {
			dpLen += l
		}

		centroidLen := 0
		for _, l := range meta.LenCentroids {
			centroidLen += l
		}

		nsLen := len(meta.LenDP)

		s := "[%v] namespaces: %3d | centroids: %6d | dps: %6d | task: %v\n"
		fmt.Printf(s, addr, nsLen, centroidLen, dpLen, task)

		totalDps += dpLen
	}

	m.pollAccuracy()
	fmt.Printf("total dps: %v\n", totalDps)
	if m.accuracyN > 0 {
		fmt.Println("accuracy fast: ", m.accuracyTotalFast/float64(m.accuracyN))
		fmt.Println("accuracy accu: ", m.accuracyTotalAccurate/float64(m.accuracyN))
	}
}

// Used for each node in the test network to register tasks and metadata
// to tMonitor.
type tLogger struct {
	addr    Addr
	monitor *tMonitor
}

func (l *tLogger) LogMeta(m MetaData) {
	l.monitor.Lock()
	defer l.monitor.Unlock()
	l.monitor.metaData[l.addr] = m
}

func (l *tLogger) LogTask(s string) {
	l.monitor.Lock()
	defer l.monitor.Unlock()
	l.monitor.taskData[l.addr] = s
}

/*
--------------------------------------------------------------------------------
CFG stuff.
--------------------------------------------------------------------------------
*/

func cfg(addr Addr, addrs []Addr, m *tMonitor) *EventLoopConfig {

	rand.Seed(time.Now().UnixNano())
	return &EventLoopConfig{
		LocalAddr:   addr,
		RemoteAddrs: addrs,

		TimeoutLoop: time.Second * 1,
		TimeoutStep: time.Millisecond * time.Duration(rand.Intn(1000)+100),

		TaskSkip: EventLoopTaskSkipConfig{
			Expire:                       rand.Intn(3) + 1,
			MemTrim:                      rand.Intn(3) + 1,
			DistributeDataPointsFast:     rand.Intn(3) + 1,
			DistributeDataPointsAccurate: rand.Intn(3) + 1,
			DistributeDataPointsInternal: rand.Intn(3) + 1,
			SplitCentroids:               rand.Intn(3) + 1,
			MergeCentroids:               rand.Intn(3) + 1,
			LoadBalancing:                rand.Intn(3) + 1,
			Meta:                         1,
		},

		DistributeDataPointsFastN:     100,
		DistributeDataPointsAccurateN: 50,
		DistributeDataPointsInternalN: 500,

		SplitCentroidsMin: 1000,
		SplitCentroidsMax: 1000000,

		MergeCentroidsMin: -1,
		MergeCentroidsMax: 100,

		LogLocalOnly: true,
		L:            &tLogger{addr: addr, monitor: m},
	}
}

/*
--------------------------------------------------------------------------------
Test funcs... well, just running the monitor, actually.
--------------------------------------------------------------------------------
*/

func TestMonitor(t *testing.T) {
	g_network.Reset()
	defer g_network.Reset()

	// abbreviations.
	dim := g_dpDim
	vMin := g_dpVecMin
	vMax := g_dpVecMax
	eMin := g_dpExpireSecMin
	eMax := g_dpExpireSecMax

	monitor := tMonitor{
		addrs:     g_addrs,
		namespace: g_namespace,
		network:   g_network,

		metaData: make(map[Addr]MetaData),
		taskData: make(map[Addr]string),
	}

	// Start event loops for each node.
	stops := make([]func(), len(g_addrs))
	for i, addr := range g_addrs {
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(1000)+100))
		stops[i] = EventLoop(cfg(addr, g_addrs, &monitor))
	}

	stopTime := time.Now().Add(time.Second * time.Duration(g_testSeconds))
	for {
		time.Sleep(time.Millisecond * 100)
		if time.Now().After(stopTime) {
			break
		}

		monitor.Refresh()

		// More dps.
		dps := dpsRand(len(g_addrs), dim, vMin, vMax, eMin, eMax)
		for _, dp := range dps {
			if err := dpAddRand(g_addrs, g_namespace, dp); err != nil {
				//t.Fatalf("failed to add more dps: %v", err)
			}
		}

		fmt.Printf("\nuntil done: %v\n", stopTime.Sub(time.Now()))
	}

	for _, stop := range stops {
		stop()
	}
}

func TestCleanup(t *testing.T) {
	g_network.Stop()
}
