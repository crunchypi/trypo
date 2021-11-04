package eventloop

import (
	"trypo/pkg/kmeans/rpc"
)

// This is a helper for grouping addresses by the namespaces they use/have.
// Useful because data is moved between multiple KMeansServer instances
// (using KMeansCLient), and it should only be moved on equal namespaces
// (so a dp in namespace A does not to go namespace B elsewhere).
type addrNamespaceTable struct {
	items map[string][]Addr
}

func (t *addrNamespaceTable) addEntry(addr Addr, namespace string) {
	if t.items == nil {
		t.items = make(map[string][]Addr)
	}

	slice, ok := t.items[namespace]
	if !ok {
		slice = make([]Addr, 0, 5)
	}

	slice = append(slice, addr)
	t.items[namespace] = slice

}

// Safe addr slice access.
func (t *addrNamespaceTable) addrsWithNamespace(namespace string) []Addr {
	slice, ok := t.items[namespace]
	if !ok {
		slice = make([]Addr, 0, 0)
	}
	return slice
}

// all keys in the map.
func (t *addrNamespaceTable) namespaces() []string {
	r := make([]string, 0, len(t.items))
	for k := range t.items {
		r = append(r, k)
	}
	return r
}

// Simply fetch the amount of datapoints nodes have in a namespace.
func fetchRemoteLenDPs(addrs []Addr, namespace string) map[Addr]int {
	type nodeDPLen struct {
		addr  Addr
		DPLen int
		ok    bool
	}

	// Fetch.
	ch := make(chan nodeDPLen, len(addrs))
	for _, addr := range addrs {
		go func(addr Addr) {
			var err error
			client := rpc.KMeansClient(addr.ToStr(), namespace, &err)
			lenDP := client.LenDP()
			// NamespaceErr can be interpreted as dp len = 0.
			_, castOK := err.(rpc.NamespaceErr)
			ok := err == nil || (err != nil && !castOK)
			ch <- nodeDPLen{addr, lenDP, ok}
		}(addr)
	}

	// Collect.
	res := make(map[Addr]int, len(addrs))
	for i := 0; i < len(addrs); i++ {
		r := <-ch
		if r.ok {
			res[r.addr] = r.DPLen
		}
	}
	return res
}
