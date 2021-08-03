package nodes

import "trypo/pkg/kmeans/rpc"

// Used as a general response from remote nodes.
type fetchVecsRes struct {
	vec  []float64
	addr Addr
	ok   bool
}

// Helper slice of fetchVecsRes, used for attaching methods.
type fetchVecsResSlice []fetchVecsRes

// Converts fetchVecsResSlice to a generator that iterates over each vec.
// meant to be used for knnSearchFunc (alias type in this pkg).
func (f *fetchVecsResSlice) intoVecGenerator() vecGenerator {
	i := 0
	return func() ([]float64, bool) {
		if i >= len(*f) {
			return nil, false
		}
		i++
		return (*f)[i-1].vec, true
	}
}

// Converts fetchVecsResSlice into contained Addr.
func (f *fetchVecsResSlice) intoAddrs() []Addr {
	res := make([]Addr, len(*f))
	for i, r := range *f {
		res[i] = r.addr
	}
	return res
}

// Holds a channel of fetchVecsRes, generally used for attaching methods.
type fetchVecsChan struct {
	ch    chan fetchVecsRes
	chlen int
}

// Unwrap channel in fetchVecsChan.
func (f *fetchVecsChan) collect() *fetchVecsResSlice {
	r := make(fetchVecsResSlice, 0, f.chlen)
	for i := 0; i < f.chlen; i++ {
		data := <-f.ch
		if data.ok {
			r = append(r, data)
		}
	}
	return &r
}

// Generally fetch some vecs from addrs, using fetchFunc. This spawns a goroutine
// for each addr in addrs, but does not wait for completion.
func fetchVecs(addrs []Addr, fetchFunc func(Addr) ([]float64, bool)) *fetchVecsChan {
	ch := make(chan fetchVecsRes, len(addrs))
	for _, addr := range addrs {
		go func(addr Addr) {
			vec, ok := fetchFunc(addr)
			ch <- fetchVecsRes{vec: vec, addr: addr, ok: ok}
		}(addr)
	}
	return &fetchVecsChan{ch: ch, chlen: len(addrs)}
}

// Used with fetchVecs to fetch remote vecs from 'addrs' using
// rpc.KmeansClient(...).Vec() (i.e fast fetch).
func fetchVecsFast(addrs []Addr, namespace string) *fetchVecsChan {
	return fetchVecs(addrs, func(addr Addr) ([]float64, bool) {
		var err error
		vec := rpc.KMeansClient(addr.ToStr(), namespace, &err).Vec()
		return vec, err == nil && vec != nil
	})
}

// Used with fetchVecs to fetch remote vecs from 'addrs' using
// rpc.KmeansClient(...).NearestCentroidVec(...), i.e a fetch with a greated
// granularity than 'fetchVecsFast' (in this pkg). To be specific, this
// uses pkg/kmeans/centroid.Centroid instead of
// pkg/kmeans/centroidmanager.CentroidManager
func fetchVecsAccurate(addrs []Addr, namespace string, qVec []float64) *fetchVecsChan {
	return fetchVecs(addrs, func(addr Addr) ([]float64, bool) {
		var err error
		client := rpc.KMeansClient(addr.ToStr(), namespace, &err)
		vec := client.NearestCentroidVec(qVec)
		return vec, err == nil && vec != nil
	})
}
