package api

import (
	"time"
	"trypo/pkg/kmeans/common"
)

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
