package eventloop

import (
	"fmt"
	"log"
	"os"
	"os/exec"
)

type Logger interface {
	LogTask(string)
	LogMeta(MetaData)
}

type MetaDataItem struct {
	LenDP        map[string]int
	LenCentroids map[string]int
}

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

	fmt.Println()
	cmd := exec.Command("clear")
	cmd.Stdout = os.Stdout
	cmd.Run()

	// Iter like this instead of l.metaData.Items because the order
	// will get weird (since it's a map).
	if len(l.metaData.Items) > 0 {
		for _, addr := range l.globalAddrs {

			dpLen := 0
			for _, l := range l.metaData.Items[addr].LenDP {
				dpLen += l
			}

			centroidLen := 0
			for _, l := range l.metaData.Items[addr].LenCentroids {
				centroidLen += l
			}

			nsLen := len(l.metaData.Items[addr].LenDP)

			line := fmt.Sprintf("[%v] namespaces: %3d | centroids: %6d | dps: %6d |",
				addr.ToStr(), nsLen, centroidLen, dpLen)

			if addr.Comp(l.localAddr) {
				line += fmt.Sprintf(" task: %v", s)
			}
			if l.localOnly && !addr.Comp(l.localAddr) {
				continue
			}

			log.Println(line)
		}
	}
}
