/*
This file and pkg is an RPC layer on top of the functionality in pkg/kmeans.
It simply has a client (and server) that implement kmeans/common/CentroidManager
interface.
*/
package rpc

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"trypo/pkg/kmeans/centroid"
	"trypo/pkg/kmeans/centroidmanager"
	"trypo/pkg/kmeans/common"
	"trypo/pkg/searchutils"
)

// Abbreviations.
type DataPoint = common.DataPoint
type Centroid = centroid.Centroid
type CentroidManager = centroidmanager.CentroidManager

// NamespaceErr is a common error that might occur while doing remote call
// through kmeansClient (defined in this pkg). A KMeansServer can hold multiple
// concrete implementations of common.CentroidManager, each accessed through
// a namespace -- this err is used for namespaces that do not exist and
// can't/won't be created.
type NamespaceErr struct{ namespace string }

func (nse NamespaceErr) Error() string {
	return fmt.Sprintf("namespace not found: '%v'", nse.namespace)
}

// Private so ot can only be used through the KMeansClient func, which forces
// an address and namespace specification.
type kmeansClient struct {
	remoteAddr string
	namespace  string
	err        *error

	// Misc; func for doing a knn search, where 'targetVec' is the vec to find k
	// nearest neighs for, 'vecs' is a generator of vectors (vector pool).
	// Return must be a slice of indexes referencing the vector pool.
	KNNSearchFunc func(targetVec []float64, vecs func() ([]float64, bool), k int) []int
	// Misc; func for creating a Centroid behind a pointer.
	CentroidFactory func(vec []float64) *Centroid
}

// KMeansClient forces a correct setup/use of the kmeansClient type, it
// contains methods that connect to a remote. It accepts a remote address,
// a data namespace, as well as an error (can be ignored with nil), which
// is generally used for either connection/network issues or NamespaceErr.
// Example:
//	var err error
// 	vec := KMeansClient("localhost:3000", "someNamespace", &err).Vec()
//	if err != nil { ... }
//
// Misc: The returned instance can have additional configuration for
// fields KNNSearchFunc and CentroidFactory. The necessity for their
// config will be mentioned for relevant methods.
func KMeansClient(remoteAddr, namespace string, err *error) *kmeansClient {
	if err == nil {
		var e error
		err = &e
	}
	return &kmeansClient{
		remoteAddr: remoteAddr,
		namespace:  namespace,
		err:        err,

		KNNSearchFunc: searchutils.KNNCos,
		CentroidFactory: func(vec []float64) *Centroid {
			centroid, _ := centroid.NewCentroid(centroid.NewCentroidArgs{
				InitVec:       vec,
				InitCap:       10,
				KNNSearchFunc: searchutils.KNNCos,
				KFNSearchFunc: searchutils.KFNCos,
			})
			return &centroid
		},
	}
}

/*
Note, below are a few types related to the rpc server implementation.
They (CManagerSlot and CManagerTable) are primarily used for data namespacing
in the context of concurrency. It generally works like so:

CManagerTable contains a map where the keys are namespaces while vals are
CManagerSlot, which keeps concrete impl of common.CentroidManager. Both have a
locking mechanism, such that one Goroutine won't lock the entire system just by
accessing one single CManagerSlot/common.CentroidManager.

So lock the table -> access slot -> unlock table.
Lock slot -> do op -> unlock slot.

*/

// CManagerSlot keeps a concrete common.CentroidManager instance.
// Safe concurrency usage done with CManagerSlot.Access.
type CManagerSlot struct {
	cManager *CentroidManager
	sync.Mutex
}

// Access does a concurrency safe operation on the internal common.CentroidManager
// data. Example:
//	x.Access(func(c common.CentroidManager) { c.Vec() } )
func (s *CManagerSlot) Access(f func(*CentroidManager)) {
	s.Lock()
	defer s.Unlock()

	f(s.cManager)
}

// CManagerTable contains namespaced CManagerSlot (i.e map). Its primary purpose
// is to prevent that a single Goroutine locks the entire system while using a slot,
// so there is a double locking mechanism (one in this type, another in CManagerSlot).
// Safe concurrent access is done with CManagerTable.Access.
type CManagerTable struct {
	slots map[string]*CManagerSlot
	sync.Mutex
}

func (t *CManagerTable) Namespaces() []string {
	t.Lock()
	defer t.Unlock()

	namespaces := make([]string, 0, len(t.slots))
	for namespace := range t.slots {
		namespaces = append(namespaces, namespace)
	}

	return namespaces
}

// Access does a concurrency safe access of the internal namespaced data in
// CManagerTable, which can be done with one Goroutine per namespace.
// Example:
//
//	# This task func is passed to CManagerSlot.Access.
//	taskF := func(c common.CentroidManager) { c.Vec()}
//	# False will be returned only if namespace doesn't exist.
//	namespaceExist := x.Access("someNamespace", taskF)
//	if !namespaceExist { ... }
func (t *CManagerTable) Access(namespace string, f func(*CentroidManager)) bool {
	// Grab lock only for the map access, the slot has another lock for accessing
	// common.CentroidManger itself.
	t.Lock()
	slot, ok := t.slots[namespace]
	t.Unlock()

	if !ok {
		return false
	}
	slot.Access(f)
	return true
}

// AddSlot safely (mutex) adds a CManagerSlot to CManagerTable. Will abort
// and return false if the CManagerSlot or the contained common.CentroidManger
// are nil.
func (t *CManagerTable) AddSlot(namespace string, cms *CManagerSlot) bool {
	if cms == nil || cms.cManager == nil {
		return false
	}

	t.Lock()
	defer t.Unlock()
	t.slots[namespace] = cms
	return true
}

// CentroidManagerFactoryF is whatever creates a CentroidManager.
type CentroidManagerFactoryF = func(vec []float64) *CentroidManager

// KMeansServer is contains endpoint counterparts for kmeansClient (accessed
// with KMeansClient(...)).
type KMeansServer struct {
	// Address associated with this server.
	addr string
	// Table with namespaced common.CentroidManager instances.
	Table *CManagerTable
	// The server has functionality for creating new namespaced CentroidManager
	// and will need a way of doing that.
	CentroidManagerFactoryFunc CentroidManagerFactoryF
}

// NewKMeansServer sets up (but doesn't start) a new KMeansServer.
func NewKMeansServer(addr string, f CentroidManagerFactoryF) *KMeansServer {
	slots := make(map[string]*CManagerSlot)
	table := CManagerTable{slots: slots}

	return &KMeansServer{
		addr:                       addr,
		Table:                      &table,
		CentroidManagerFactoryFunc: f,
	}
}

// StartListen is a convenience func for starting one or more instances of
// KMeansServer -- it is not a method of that type because that would make
// Go complain (since it is an RPC server). Will return a func that can be
// used to stop a server.
func StartListen(s *KMeansServer) (stop func(), err error) {
	handler := rpc.NewServer()
	handler.Register(s)

	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return nil, err
	}

	var conn net.Conn
	stop = func() {
		ln.Close()
		if conn != nil {
			conn.Close()
		}
	}

	go func() {
		for {
			cxn, err := ln.Accept()
			conn = cxn
			if err != nil {
				break
			}
			go handler.ServeConn(cxn)
		}
	}()
	return stop, nil
}
