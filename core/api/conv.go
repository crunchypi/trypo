package api

import (
	"strings"
	"time"
	"trypo/pkg/kmeans/common"
)

// conv []String -> []Addr
func strsToAddrs(ss []string) ([]Addr, bool) {
	r := make([]Addr, len(ss))
	for i, s := range ss {
		ipAddr := strings.Split(s, ":")
		if len(ipAddr) != 2 {
			return r, false
		}
		r[i] = Addr{IP: ipAddr[0], Port: ipAddr[1]}
	}
	return r, true
}

// conv []Addr -> []String
func addrsToStrs(addrs []Addr) []string {
	r := make([]string, len(addrs))
	for i, v := range addrs {
		r[i] = v.ToStr()
	}
	return r
}

// Same as common.DataPoint (pkg/kmeans/common/datapoint.go) but with json tags.
type DP struct {
	Vec           []float64 `json:"vec"`
	Payload       []byte    `json:"payload"`
	Expires       time.Time `json:"expires"`
	ExpireEnabled bool      `json:"expireEnabled"`
}

// conv DP -> common.DataPoint (pkg/kmeans/common/datapoint.go)
func (dp *DP) toDataPoint() common.DataPoint {
	return common.DataPoint{
		Vec:           dp.Vec,
		Payload:       dp.Payload,
		Expires:       dp.Expires,
		ExpireEnabled: dp.ExpireEnabled,
	}
}

// conv []common.DataPoint (pkg/kmeans/common/datapoint.go) -> []DP.
func DataPointsToDPs(dps []common.DataPoint) []DP {
	r := make([]DP, len(dps))
	for i, dp := range dps {
		r[i] = DP{
			Vec:           dp.Vec,
			Payload:       dp.Payload,
			Expires:       dp.Expires,
			ExpireEnabled: dp.ExpireEnabled,
		}
	}
	return r
}
