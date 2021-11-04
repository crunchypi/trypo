# trypo

Trypo abbreviates trypophobia and is a distributed data-store for approximate nearest neighbour (ANN) searching, using k-means as a space-partitioning technique, as opposed to LSH or tree-based solutions. The project is an unoptimized prototype and should not be used for anything serious, though it'll get there someday.

# Usage

Generally, one would need to add some network info and other parameters in /cfg/cfg.go. Then, a local node can be started by running /cmd/service/service.go and the (network) API can be interfaced with JSON/POST (usage described further down in this README). Note that the system is self-correcting and the network will work itself into a state of accuracy over time (not that it necessarily starts off with low accuracy).

More detailed steps:
- Open /cfg/cfg.go
- Specify all addresses for nodes in the network ('OtherAddrRPC'), should include the local one.
- Assign the local RPC address with 'LocalAddrRPC'.
- Assign the local API endpoint addr with 'LocalAddrAPI'
- Run `go run .` while in /cmd/service/ to start a local node.


# API
The API is JSON over POST and has two very simple ways of interacting with the system: insert and lookup. Inserting data is done by sending a JSON with the following form to the `addr/port/api/dp/put` endpoint:
```
{
  namespace: "abs",         // data can be segmented by a namespace.
  accurate: true,           // Placement accuracy.
  dp: {
    vec: [0, 1.1, 3],       // numeric vector.
    payload: []             // byte array. 
    expires:  xyz,          // Time compatible with time.Time of Go.
    expireEnabled: true     // Whether or not the dp can expire.
  }
}
```

Note that this will send the data to the node specified in the URL but the data will be forwarded somewhere apprpriate with regards to accuracy. For instance, the 'accurate' field as false will make the recieving node look through all nodes in the network and check the the overall mean/average point for the relevant namespace (recall, this is k-means based, so it's the mean of all centroids). The node with the closest mean will be assigned the new data. If that field is false, on the other hand, then the granularity level will be as all centroids in all nodes, which will be more precise but costly. Either way, data is self-correcting in the network so the faster/inaccurate(relatively) approach is recommended.

For data retrieval (nearest neighbours), the endpoint `addr/port/api/dp/query` is used with this JSON format:
```
{
  namespace: "abc"        // Same as for data placement above.
  accurate: true          // Same as for data placement above.
  queryVec: [1,0,3.2]     // Get data similar to this.
  n : 3                   // the K in KNN.
  drain: false            // true will remove the data in the system.
}



