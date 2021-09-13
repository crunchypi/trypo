package eventloop

import (
	"fmt"
	"log"
	"os"
	"os/exec"
)

// Logger is a logger for the event-loop used in this pkg. It is primarily used
// in EventLoopConfig. See method-specific docs for more details.
type Logger interface {
	// LogTask is called rapidly for each event-loop task, with a string
	// containing the task name and some additional info such as namespaces.
	LogTask(string)
	// LogMeta is intended to be used for system monitoring, with data pulled
	// from either the local node, or all nodes in the network, depending on
	// the LogLocalOnly field in EventLoopConfig. System data is stored in,
	// and accessed through the MetaData arg here. More info about the data
	// itself is documented on the MetaData type.
	LogMeta(MetaData)
}

// MetaDataItem contains some metadata for a node (pkg/kmeans/rpc). The two
// fields, LenDP and LenCentroids, are maps where keys are namespaces and
// vals contain amounts of data.
type MetaDataItem struct {
	// LenDP is a map where keys are namespaces and vals are the amounts of
	// datapoints stored in the node behind the aforementioned namespace.
	LenDP        map[string]int
	LenCentroids map[string]int
	// LenCentroids is a map where keys are namespaces and vals are the amounts
	// of centroids stored in the nod ebehind the aforementioned namespace.
}

// MetaData is used for system info that is logged in the Logger interface  in
// this pkg. It has a single field (Items) which simply is a map where keys are
// addresses (for local and remote nodes) and vals are MetaDataItem. The map will
// be populated depending on EventLoopConfig.LogLocalOnly (so if true, then the
// only key-val pair will be for the local node. See doc for that field for more
// details).
type MetaData struct {
	Items map[Addr]MetaDataItem
}

type defaultLogger struct {
	localOnly   bool
	localAddr   Addr
	globalAddrs []Addr
	metaData    MetaData
}

func (l *defaultLogger) LogMeta(m MetaData) { l.metaData = m }

func (l *defaultLogger) LogTask(s string) {
	if len(l.metaData.Items) == 0 {
		log.Printf("[%v] task: %v", l.localAddr.ToStr(), s)
		return
	}

	// @ Only unix-based.
	fmt.Println()
	cmd := exec.Command("clear")
	cmd.Stdout = os.Stdout
	cmd.Run()

	if len(l.metaData.Items) > 0 {
		// Iter like this instead of l.metaData.Items because the order
		// will get weird/inconsistent (since it's a map).
		for _, addr := range l.globalAddrs {
			// localOnly=true will only pull metadata from the local node. So
			// if that's true, then it would be a bit ugly and unnecessary to
			// print data for other nodes.
			if l.localOnly && !addr.Comp(l.localAddr) {
				continue
			}

			// Collect total amount of dps in the node (for all namespaces).
			dpLen := 0
			for _, l := range l.metaData.Items[addr].LenDP {
				dpLen += l
			}

			// Collect total amount of centroids in the node (for all namespaces).
			centroidLen := 0
			for _, l := range l.metaData.Items[addr].LenCentroids {
				centroidLen += l
			}

			nsLen := len(l.metaData.Items[addr].LenDP)

			// Node data.
			line := fmt.Sprintf("[%v] namespaces: %3d | centroids: %6d | dps: %6d |",
				addr.ToStr(), nsLen, centroidLen, dpLen)

			// Add current process description to node data this node is local.
			if addr.Comp(l.localAddr) {
				line += fmt.Sprintf(" task: %v", s)
			}

			log.Println(line)
		}
	}
}
