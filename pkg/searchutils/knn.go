/*
This file contains a few funcs which do 'normal' k-nearest (or furthest) neighs
searching using vectors. The main implementation is with KNNBrute(...), while
the other exported funcs (below it) are just convenience funcs/prefabs which
configure KNNBrute.

*/

package searchutils

import (
	"math"
	"trypo/pkg/mathutils"
)

// Internal type for tracking searched elements that are best..
type resultItem struct {
	// The search funcs essentially operate on iterables (currently
	// generators) and return a slice of indexes which represent
	// elements in those iterables. This var represent those indexes.
	index int
	// Used in search funcs to keep track of vector relevance.
	score float64
	// Used a signal for whether or not the instance of resultItem
	// is actually used and not just initialised.
	set bool
}

// bubble inserts the 'insertee' into 'items' in an ordered manner (in place),
// without changing the length of 'items' (i.e a value will be lost). The order
// is specified with the related arg. Note: only works as expected only if the
// 'items' slice is already sorted.
// 		Example(0, [1,2,3], true) -> [0,1,2]
// 		Example(3, [2,1,0], false) -> [3,2,1]
func bubble(insertee *resultItem, items []resultItem, ascending bool) {
	for i := 0; i < len(items); i++ {
		// Clarification: '|| !items[i].set' specifies that score can be set if the item is inactive.
		if (insertee.score > items[i].score || !items[i].set) && !ascending {
			*insertee, items[i] = items[i], *insertee
		}
		// Clarification: '|| !items[i].set' specifies that score can be set if the item is inactive.
		if (insertee.score < items[i].score || !items[i].set) && ascending {
			*insertee, items[i] = items[i], *insertee
		}
	}
}

// resItems2Indexes simply converts a slice of resultItems to a slice of contained index values.
func resItems2Indexes(items []resultItem) []int {
	res := make([]int, 0, len(items))
	for i := 0; i < len(items); i++ {
		if items[i].set {
			res = append(res, items[i].index)
		}
	}
	return res
}

// KNNBruteArgs contain arguments for KNNBrute. All args must be specified.
type KNNBruteArgs struct {
	// In a KNN scenario, this specifies what neighs must be near to.
	TargetVec []float64
	// Intended to be a generator which returns all possible vectors that
	// TargetVec will be compared to (bool=false signals end of iterable).
	// A generator is used because it makes the search funcs more generic,
	// without having the issue with []T -> []U conversion in Go.
	VecPoolGenerator func() ([]float64, bool)
	// In a KNN scenario, this specifies the K.
	K int
	// Specifies how the KNN search funcs will evaluate significance
	// of neigs. For instance, when comparing TargetVec to potential
	// neighs using a DistFunc (next var below), does a smaller num
	// represent higher similarity? If so, then the order is Ascending.
	// Example: If using Euclidean distance as DistFunc, then smaller
	// is better and Ascending should be true. However, if the caller
	// intends to find elements in VecPoolGenerator that are furthest
	// away from TargetVec, then Ascending should be false.
	Ascending bool
	// Distance/Similarity function for comparing vectors.
	// Examples: Euclidean distance, cosine similarity, etc.
	// Note, this is paired with the Ascending field.
	DistFunc func(v1, v2 []float64) (float64, error)
}

// KNNBrute is a general-purpose linear search for finding k nearest
// (or furthest) neighs of a vector, and then returning their index.
// See KNNBruteArgs (accepted argument) for more info.
func KNNBrute(args KNNBruteArgs) []int {
	res := make([]resultItem, args.K)
	// This represents the lowest/highest score found so far.
	// Defining a cap of highest/lowest number representing similarity
	// is done here by simply using the (near) min/max allowed float.
	similarity := math.MaxFloat64
	if !args.Ascending {
		similarity *= -1
	}
	// Apply worst score to all resultItems.
	for i := 0; i < args.K; i++ {
		res[i].score = similarity
	}
	i := 0
	for {
		// Next vector.
		v, cont := args.VecPoolGenerator()
		if !cont {
			break
		}
		// Next score.
		score, err := args.DistFunc(args.TargetVec, v)
		if err != nil {
			i++
			continue
		}
		// Evaluate inclusion of current vector.
		newSlot := &resultItem{i, score, true}
		if args.Ascending && score < similarity {
			bubble(newSlot, res, true)
		}
		if !args.Ascending && score > similarity {
			bubble(newSlot, res, false)
		}
		i++
	}
	return resItems2Indexes(res)
}

// KNNCos finds 'k' nearest neighs using cosine similarity. It accepts 'targetVec' which
// is compared to all vectors given by 'vecPoolGenerator' (bool=false signals stop).
// The return is a slice of indexes referencing the nearest neighs.
func KNNCos(targetVec []float64, vecPoolGenerator func() ([]float64, bool), k int) []int {
	return KNNBrute(KNNBruteArgs{
		TargetVec:        targetVec,
		VecPoolGenerator: vecPoolGenerator,
		K:                k,
		Ascending:        false,
		DistFunc:         mathutils.CosineSimilarity,
	})
}

// KFNCos is a counterpart of KNNCos which finds K furthest neighs instead of nearest.
func KFNCos(targetVec []float64, vecPoolGenerator func() ([]float64, bool), k int) []int {
	return KNNBrute(KNNBruteArgs{
		TargetVec:        targetVec,
		VecPoolGenerator: vecPoolGenerator,
		K:                k,
		Ascending:        true,
		DistFunc:         mathutils.CosineSimilarity,
	})
}

// KNNEuc finds 'k' nearest neighs using Euclidean distance. It accepts 'targetVec' which
// is compared to all vectors given by 'vecPoolGenerator' (bool=false signals stop).
// The return is a slice of indexes referencing the nearest neighs.
func KNNEuc(targetVec []float64, vecPoolGenerator func() ([]float64, bool), k int) []int {
	return KNNBrute(KNNBruteArgs{
		TargetVec:        targetVec,
		VecPoolGenerator: vecPoolGenerator,
		K:                k,
		Ascending:        true,
		DistFunc:         mathutils.EuclideanDistance,
	})
}

// KFNEuc is a counterpart of KNNEuc which finds k furthest neighs instead of nearest.
func KFNEuc(targetVec []float64, vecPoolGenerator func() ([]float64, bool), k int) []int {
	return KNNBrute(KNNBruteArgs{
		TargetVec:        targetVec,
		VecPoolGenerator: vecPoolGenerator,
		K:                k,
		Ascending:        false,
		DistFunc:         mathutils.EuclideanDistance,
	})
}
