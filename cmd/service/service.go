package main

import (
	"time"
	"trypo/cfg"
	"trypo/core/api"
	"trypo/core/eventloop"
	"trypo/pkg/kmeans/centroidmanager"
	"trypo/pkg/kmeans/rpc"
)

func main() {

	// Used for spawning CentroidManager instances by the rpc node,
	cmSpawner := func(vec []float64) *centroidmanager.CentroidManager {
		args := centroidmanager.NewCentroidManagerArgs{
			InitVec:             vec,
			InitCap:             cfg.KMEANS_INITCAP,
			CentroidDPThreshold: cfg.KMEANS_CENTROID_DP_THRESHOLD,
			KNNSearchFunc:       cfg.KNN_SEARCH_FUNC,
			KFNSearchFunc:       cfg.KFN_SEARCH_FUNC,
		}
		cm, ok := centroidmanager.NewCentroidManager(args)
		if !ok {
			panic("cmSpawner failed to spawn.")
		}
		return &cm
	}

	// RPC node spawn.
	rpcNode := rpc.NewKMeansServer(cfg.LocalAddrRPC.ToStr(), cmSpawner)
	rpcStop, err := rpc.StartListen(rpcNode)
	if err != nil {
		panic("failed to start rpc node")
	}
	defer rpcStop()

	// Will panic by itself if setup is shabby.
	eltStop := eventloop.EventLoop(&cfg.ELT)
	defer eltStop()

	// WAPI for user-facing interface.
	err = api.Start(api.APIConfig{
		Addr:         cfg.LocalAddrAPI,
		RPCAddrs:     cfg.OtherAddrRPC,
		ReadTimeout:  time.Second * 5,
		WriteTimeout: time.Second * 5,
	})
	if err != nil {
		panic(err)
	}
}
