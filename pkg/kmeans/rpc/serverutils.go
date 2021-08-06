/*
Some utils for fetching remote vectors.
*/
package rpc

// Readability alias: standard generator that returns vectors (false=stop).
type vecGenerator = func() ([]float64, bool)

// Readability alias: a func that finds 'k' nearest neighs (vecs) of 'targetVec'.
type knnSearchFunc = func(targetVec []float64, vecs vecGenerator, k int) []int

// Used as a vector response from remote nodes.
type fetchVecsRes struct {
	vec  []float64
	addr string
	ok   bool
}

// Helper slice of fetchVecsRes, used for attaching methods.
type fetchVecsResSlice []fetchVecsRes

// Converts fetchVecsResSlice to a generator that iterates over each vec.
// meant to be used for knnSearchFunc (alias type in this file).
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

// Converts fetchVecsResSlice into contained addr strings.
func (f *fetchVecsResSlice) intoAddrs() []string {
	res := make([]string, len(*f))
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
func fetchVecs(addrs []string, fetchFunc func(string) ([]float64, bool)) *fetchVecsChan {
	ch := make(chan fetchVecsRes, len(addrs))
	for _, addr := range addrs {
		go func(addr string) {
			vec, ok := fetchFunc(addr)
			ch <- fetchVecsRes{vec: vec, addr: addr, ok: ok}
		}(addr)
	}
	return &fetchVecsChan{ch: ch, chlen: len(addrs)}
}
