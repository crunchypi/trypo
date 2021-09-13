/*
This pkg defines some helpers for adding/removing(searching) datapoints to/from
remote nodes (pkg/kmeans/rpc) conveniently. At the moment of writing, there
are three ways of doing each; (1) randomly, (2) fast with some accuracy, and
(3) slow with a lot of accuracy. accurate. Variation 2 will put/get datapoints
into nodes based on the vector of CentroidManager, while variant 3 will use
Centroid (contained by CentroidManager) as granularity, which is slower but
much more accurate.
*/
package dps

import (
	"math/rand"
	"time"
	"trypo/pkg/arbiter"
	"trypo/pkg/kmeans/common"
)

type Addr = arbiter.Addr
type DataPoint = common.DataPoint

type vecGenerator = func() ([]float64, bool)
type knnSearchFunc = func(targetVec []float64, vecs vecGenerator, k int) []int

func shuffleAddrs(addrs []Addr) []Addr {
	res := make([]Addr, len(addrs))
	copy(res, addrs)

	for i := 0; i < len(addrs); i++ {
		rand.Seed(time.Now().UnixNano())
		j := rand.Intn(len(addrs))
		res[i], res[j] = res[j], res[i]
	}
	return res
}
