package eventloop

import "time"

// Each task in the event loop will be skippable such that not everything has
// to run in each loop iteration. This is useful when some tasks are recource
// and/or time intensive (more than others) and should run infrequently.
// Example:
//	int=1 : run on each loop.
//	int=2 : run every second loop.
//	int=3 : run every third loop.
//	etc...
// Values over 1000 will never be ran.
type EventLoopTaskSkipConfig struct {
	// Expire triggers datapoint expiration in the whole network.
	// Not necessary to always do this, as moving dataponts will
	// often auto-expire them, though this isn't garanteed.
	Expire int
	// MemTrim triggers memory reduction in the whole network.
	// In practice, this means that slices containing datapoints
	// and centroids have their capacity reduced. Not necessary
	// to always do this, since slices might be re-populated with
	// new data, thout that is not garuanteed.
	MemTrim int
	// DistributeDataPointsFast triggers the hasty movement of
	// datapoints within the network. It is a way of attracting
	// dps to some best-fit node, but with an accuracy/speed
	// tradeoff (also see DistributeDataPointsFast field). It
	// moved dps on a node granularity. The actual amount to
	// distribute is set in EventLoopConfig.
	DistributeDataPointsFast int
	// DistributeDataPointsAccurate triggers movement of datapoints
	// within the network with highest possible accuracy, though at
	// the cost of speed. It is an alternative to DistributeDataPointsFast
	// and works on a Centroid granularity (contained by nodes), as opposed
	// to just nodes. The actual amount to distribute is set in EventLoopConfig.
	DistributeDataPointsAccurate int
	// DistributeDataPointsInternal triggers movement of datapoints
	// within each node in the network, as opposed to between nodes.
	// It can be thought of as data integrity on a node-level. The
	// actual amount to distribute is set in EventLoopConfig.
	DistributeDataPointsInternal int
	// SplitCentroids triggers procedures in the network that splits
	// centroids if they are too big. The threshold values are specified
	// in EventLoopConfig.
	SplitCentroids int
	// MergeCentroids triggers procedures in the network that merges
	// centroids if they are too small. The threshold values are
	// specified in EventLoopConfig.
	MergeCentroids int
	// LoadBalancing triggers load-balancing in the network.
	LoadBalancing int
}

// Clams vals in EventLoopTaskSkipConfig (particularly useful for
// ensuring no zeros (zero div err when using modulus))
func (cfg *EventLoopTaskSkipConfig) clamp(min, max int) {
	items := []*int{
		&cfg.Expire,
		&cfg.MemTrim,
		&cfg.DistributeDataPointsFast,
		&cfg.DistributeDataPointsAccurate,
		&cfg.DistributeDataPointsInternal,
		&cfg.SplitCentroids,
		&cfg.MergeCentroids,
		&cfg.LoadBalancing,
	}
	for _, v := range items {
		if *v < min {
			*v = min
		}
		if *v > max {
			*v = max
		}
	}
}

type eventLoopInternal struct {
	stopped bool
	iter    int
}

type EventLoopConfig struct {
	LocalAddr Addr
	// All addresses in the network, should include LocalAddr.
	RemoteAddrs []Addr

	// Timeout for each loop interation.
	TimeoutLoop time.Duration
	// Timeout for each task in the event loop.
	TimeoutStep time.Duration

	// Each task in the event loop will be skippable. See doc for
	// EventLoopTaskSkipConfig for more details.
	TaskSkip EventLoopTaskSkipConfig

	// Specifies how many datapoints each node in the network should
	// distribute in haste (for data integrity in the network). This
	// distribution variant works on a _node_ granularity. See also
	// the DistributeDataPointsAccurate variant.
	DistributeDataPointsFastN int
	// Specifies how many datapoints each node in the network should
	// distribute with accuracy (for data integrity in the network).
	// This distribution variant works on a _centroid_ (as opposed
	// to node) granularity (Centroids are a detail level below nodes).
	DistributeDataPointsAccurateN int
	// Specifies how many datapoints each node in the network should
	// distribute internally (node and not network data integrity).
	DistributeDataPointsInternalN int

	// Centroids within a node can grow to big and should be split
	// eventually. This is the lower end of the range in which centroids
	// will be split.
	SplitCentroidsMin int
	// Centroids within a node can grow to big and should be split
	// eventually. This is the upper end of the range in which centroids
	// will be split.
	SplitCentroidsMax int

	// Centroid within a node can be reduced over time for various
	// reasons (such as when contained datapoints are expired), and
	// should be merged eventually. This is the lower end of the range
	// in which centroids will be merged.
	MergeCentroidsMin int
	// Centroid within a node can be reduced over time for various
	// reasons (such as when contained datapoints are expired), and
	// should be merged eventually. This is the upper end of the range
	// in which centroids will be merged.
	MergeCentroidsMax int

	LogLocalOnly bool
	L            Logger

	// Added by event loop.
	internal eventLoopInternal
}

func (cfg *EventLoopConfig) validate() {
	if cfg == nil {
		panic("nil eventloop cfg")
	}
	if cfg.L == nil {
		cfg.L = &defaultLogger{
			localOnly:   cfg.LogLocalOnly,
			localAddr:   cfg.LocalAddr,
			globalAddrs: cfg.RemoteAddrs,
		}
	}

	cfg.TaskSkip.clamp(1, 1000)
}
