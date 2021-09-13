/*
Test utils for the core pkg. Contains stuff such as a test network
with nodes consisting of KMeansServer (pkg/kmeans/rpc/) as well as
ArbiterServer (pkg/arbiter).
*/
package testutils

import (
	"fmt"
	"net"
	"net/rpc"
	"time"
	"trypo/pkg/arbiter"
	"trypo/pkg/kmeans/centroid"
	"trypo/pkg/kmeans/centroidmanager"
	kmrpc "trypo/pkg/kmeans/rpc"
	"trypo/pkg/searchutils"
)

// Abbreviations.
var _knnSearchFunc = searchutils.KNNCos
var _kfnSearchFunc = searchutils.KFNCos

type Centroid = centroid.Centroid
type CentroidManager = centroidmanager.CentroidManager
type KMeansServer = kmrpc.KMeansServer
type ArbiterServer = arbiter.ArbiterServer

type Addr = arbiter.Addr

// NewCentroid creates a centroid.Centroid prefab.
func NewCentroid(vec []float64) *Centroid {
	args := centroid.NewCentroidArgs{
		InitVec:       vec,
		InitCap:       10,
		KNNSearchFunc: _knnSearchFunc,
		KFNSearchFunc: _kfnSearchFunc,
	}
	centroid, ok := centroid.NewCentroid(args)
	if !ok {
		panic("couldn't setup Centroid")
	}
	return &centroid
}

// NewCentroidManager creates a centroidmanager.CentroidManager prefab.
func NewCentroidManager(vec []float64) *CentroidManager {
	args := centroidmanager.NewCentroidManagerArgs{
		InitVec:             vec,
		InitCap:             0,
		CentroidDPThreshold: 10,
		KNNSearchFunc:       _knnSearchFunc,
		KFNSearchFunc:       _kfnSearchFunc,
	}
	cm, ok := centroidmanager.NewCentroidManager(args)
	if !ok {
		panic("couldn't setup CentroidManager for test")
	}
	return &cm
}

// NewKMeansServer creates a KmeansServer prefab,
func NewKMeansServer(addr string) *KMeansServer {
	return kmrpc.NewKMeansServer(addr, NewCentroidManager)
}

// Node is a node in TNetwork. Contains both data (KmeansServer)
// and network sync (ArbiterServer) servers.
type Node struct {
	Addr          Addr
	KMeansServer  *KMeansServer
	ArbiterServer *ArbiterServer
	StopFunc      func()
}

// StartListen makes a Node active.
func (n Node) StartListen() error {
	handler := rpc.NewServer()
	handler.Register(n.KMeansServer)
	handler.Register(n.ArbiterServer)

	ln, err := net.Listen("tcp", n.Addr.ToStr())
	if err != nil {
		return err
	}

	var conn net.Conn
	var stopped bool
	n.StopFunc = func() {
		ln.Close()
		if conn != nil {
			conn.Close()
		}
		stopped = true
	}

	go func() {
		for {
			if stopped {
				break
			}
			cxn, err := ln.Accept()
			conn = cxn
			if err != nil {
				break
			}
			go handler.ServeConn(cxn)
		}
	}()
	return nil
}

// TNetwork is a test network.
type TNetwork struct {
	Addrs []Addr
	Nodes map[Addr]*Node
}

// NewTNetwork creates a new test network.
func NewTNetwork(addrs []Addr) TNetwork {
	nodes := make(map[Addr]*Node)

	for _, addr := range addrs {
		node := Node{
			Addr:         addr,
			KMeansServer: NewKMeansServer(addr.ToStr()),
			ArbiterServer: arbiter.NewArbiterServer(arbiter.NewSessionMemberConfig{
				LocalAddr:       addr,
				Whitelist:       addrs,
				SessionDuration: time.Second * 3,
				ArbiterDuration: time.Second * 5,
			}),
		}

		startErr := node.StartListen()
		if startErr != nil {
			panic(fmt.Sprintf("couldn't start server on addr %v", addr))
		}

		nodes[addr] = &node
	}

	return TNetwork{Addrs: addrs, Nodes: nodes}
}

// Stop stops the test network (kills servers).
func (tn *TNetwork) Stop() {
	for _, n := range tn.Nodes {
		if n.StopFunc != nil {
			n.StopFunc()
		}
	}
}

// Reset resets data in a test network (doesn't kill servers).
func (tn *TNetwork) Reset() {
	for _, node := range *&tn.Nodes {
		node.KMeansServer.Table.Reset()

		sessMemb := arbiter.NewSessionMember(arbiter.NewSessionMemberConfig{
			LocalAddr:       node.Addr,
			Whitelist:       tn.Addrs,
			SessionDuration: time.Second * 3,
			ArbiterDuration: time.Second * 5,
		})

		node.ArbiterServer.ArbiterSessionMember = sessMemb
	}
}

// convenience for accessing data in test network. Not safu.
func (tn *TNetwork) UnwrapCM(addr Addr, namespace string) *CentroidManager {
	var r *CentroidManager
	node := tn.Nodes[addr]
	node.KMeansServer.Table.Access(namespace, func(cm *CentroidManager) {
		r = cm
	})
	return r
}
