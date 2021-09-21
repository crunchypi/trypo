/*
This pkg is a system-wide configuration, no detail is too large or small,
all inclusive and nicely global.

*/
package cfg

import (
	"time"
	"trypo/core/eventloop"
	"trypo/pkg/arbiter"
	"trypo/pkg/searchutils"
)

// Alias.
type Addr = arbiter.Addr

/*
--------------------------------------------------------------------------------
	This is used further down, just keeping it here at the top for easy
	access (cfg files are long and spooky).
--------------------------------------------------------------------------------
*/
// Local address for RPC network (so distributed ops).
var LocalAddrRPC = Addr{"localhost", "3500"}

// All other addresses in the RPC network. Include local.
var OtherAddrRPC = []Addr{
	LocalAddrRPC,
}

// Address for the API / web server used as a user-facing interface.
var LocalAddrAPI = Addr{"localhost", "3501"}

/*
--------------------------------------------------------------------------------
	These are search funcs for "k nearest neighbours", basically the
	entire point of the system (approximate nearest neighbours).
--------------------------------------------------------------------------------
*/

// Anyway, this is for cosine similarity and should _NOT_ be changed
// (to the XXXEuc variant) at the moment of writing because cosine
// simi is hardcoded in a couple places, and using XXXEuc here
// will mix alot of things together weirdly........................
var KNN_SEARCH_FUNC = searchutils.KNNCos

// See comment for KNN_SEARCH_FUNC in this pkg.
var KFN_SEARCH_FUNC = searchutils.KFNCos

/*
--------------------------------------------------------------------------------
	NOTE: Copied all var/field documentation from core/eventloop/cfg.go,
	keep that in mind when reading comments, because some of them will
	do something odd like saying '.. can find it in this pkg'.

	This is basically what goes into the RPC network eventloop for
	this node.
--------------------------------------------------------------------------------
*/
// Config for core/eventloop/cfg.go
var ELT = eventloop.EventLoopConfig{
	LocalAddr: LocalAddrRPC,
	// All addresses in the network, should include LocalAddr.
	RemoteAddrs: OtherAddrRPC,

	// Timeout for each loop interation.
	TimeoutLoop: time.Second * 5,
	// Timeout for each task in the event loop.
	TimeoutStep: time.Second * 5,

	// Each task in the event loop will be skippable such that not everything has
	// to run in each loop iteration. This is useful when some tasks are recource
	// and/or time intensive (more than others) and should run infrequently.
	// Example:
	//	int=1 : run on each loop.
	//	int=2 : run every second loop.
	//	int=3 : run every third loop.
	//	etc...
	// Values over 1000 will never be ran.
	TaskSkip: eventloop.EventLoopTaskSkipConfig{
		// Expire triggers datapoint expiration in the whole network.
		// Not necessary to always do this, as moving dataponts will
		// often auto-expire them, though this isn't garanteed.
		Expire: 20,
		// MemTrim triggers memory reduction in the whole network.
		// In practice, this means that slices containing datapoints
		// and centroids have their capacity reduced. Not necessary
		// to always do this, since slices might be re-populated with
		// new data, thout that is not garuanteed.
		MemTrim: 10,
		// DistributeDataPointsFast triggers the hasty movement of
		// datapoints within the network. It is a way of attracting
		// dps to some best-fit node, but with an accuracy/speed
		// tradeoff (also see DistributeDataPointsFast field). It
		// moved dps on a node granularity. The actual amount to
		// distribute is set in EventLoopConfig.
		DistributeDataPointsFast: 4,
		// DistributeDataPointsAccurate triggers movement of datapoints
		// within the network with highest possible accuracy, though at
		// the cost of speed. It is an alternative to DistributeDataPointsFast
		// and works on a Centroid granularity (contained by nodes), as opposed
		// to just nodes. The actual amount to distribute is set in EventLoopConfig.
		DistributeDataPointsAccurate: 8,
		// DistributeDataPointsInternal triggers movement of datapoints
		// within each node in the network, as opposed to between nodes.
		// It can be thought of as data integrity on a node-level. The
		// actual amount to distribute is set in EventLoopConfig.
		DistributeDataPointsInternal: 3,
		// SplitCentroids triggers procedures in the network that splits
		// centroids if they are too big. The threshold values are specified
		// in EventLoopConfig.
		SplitCentroids: 3,
		// MergeCentroids triggers procedures in the network that merges
		// centroids if they are too small. The threshold values are
		// specified in EventLoopConfig.
		MergeCentroids: 3,
		// LoadBalancing triggers load-balancing in the network.
		LoadBalancing: 7,
		// Meta triggers polling of metadata for the logger ('L' field in
		// EventLoopConfig, data is passed to the LogMeta method).
		Meta: 1,
	},

	// Specifies how many datapoints each node in the network should
	// distribute in haste (for data integrity in the network). This
	// distribution variant works on a _node_ granularity. See also
	// the DistributeDataPointsAccurate variant.
	DistributeDataPointsFastN: 100,
	// Specifies how many datapoints each node in the network should
	// distribute with accuracy (for data integrity in the network).
	// This distribution variant works on a _centroid_ (as opposed
	// to node) granularity (Centroids are a detail level below nodes).
	DistributeDataPointsAccurateN: 50,
	// Specifies how many datapoints each node in the network should
	// distribute internally (node and not network data integrity).
	DistributeDataPointsInternalN: 200,

	// Centroids within a node can grow to big and should be split
	// eventually. This is the lower end of the range in which centroids
	// will be split.
	SplitCentroidsMin: 1000,
	// Centroids within a node can grow to big and should be split
	// eventually. This is the upper end of the range in which centroids
	// will be split.
	SplitCentroidsMax: 1000000,

	// Centroid within a node can be reduced over time for various
	// reasons (such as when contained datapoints are expired), and
	// should be merged eventually. This is the lower end of the range
	// in which centroids will be merged.
	MergeCentroidsMin: -1, // Inclusive range for some reason.....
	// Centroid within a node can be reduced over time for various
	// reasons (such as when contained datapoints are expired), and
	// should be merged eventually. This is the upper end of the range
	// in which centroids will be merged.
	MergeCentroidsMax: 100,

	// The logger interface in this pkg has two methods, on of them
	// (named 'LogMeta') receves a MetaData type as arg, which has
	// some metadata for nodes. This metadata is pulled from the
	// network -- if this field LogLocalOnly=true, then data is only
	// pulled from the local node (with addr 'LocalAddr', which is
	// set in this config type). If LogLocalOnly=false, then metadata
	// is pulled from the entire network (addrs in 'RemoteAddrs', also
	// defined in this config type). Also see docs for Logger interface
	// and MetaData type.
	LogLocalOnly: true,
	// Logger for the event loop. See docs for Logger interface. If nil,
	// then this field is set as a default logger in this pkg, it'll simply
	// log events in the terminal. At the moment of writing, the default
	// logger is only compatible with unix-based systems.
	L: nil,
}

/*
--------------------------------------------------------------------------------
	Some cfg for creation of CentroidManager (pkg/kmeans/centroidmanager)
	instances, these will be used in the rpc server (pkg/kmeans/roc).
--------------------------------------------------------------------------------
*/

// This specifies the capacity of the centroid slice in each kmeansmanager instance.
var KMEANS_INITCAP = 100

// This specifies how many datapoints a centroid can have before it is split in half.
var KMEANS_CENTROID_DP_THRESHOLD = 10000
