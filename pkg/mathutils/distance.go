/*
This file contains a few functions which helps with finding 'similarity'
between vectors (such as euclidean sitance and cosine similarity).

*/

package mathutils

import (
	"errors"
	"math"
)

// EuclideanDistance finds the euclidean distance between two vectors.
// Returns an err if the vectors are of different length.
func EuclideanDistance(v1, v2 []float64) (float64, error) {
	if len(v1) != len(v2) {
		return 0, errors.New("distance measurement attempt failed: vectors are of different lengths")
	}
	var r float64
	for i := 0; i < len(v1); i++ {
		r += math.Sqrt((v1[i] - v2[i]) * (v1[i] - v2[i]))
	}
	return r, nil
}

// norm computes the norm (math) of a vec.
func norm(vec []float64) float64 {
	var x float64
	for i := 0; i < len(vec); i++ {
		x += vec[i] * vec[i]
	}
	return math.Sqrt(x)
}

// CosineSimilarity finds the cosine similarity of two vectors.
// Returns an err if the vectors are of different lengths.
func CosineSimilarity(vec1, vec2 []float64) (float64, error) {
	if len(vec1) != len(vec2) {
		return 0, errors.New("similarity measurement attempt failed: vectors are of different lengths")
	}
	norm1, norm2 := norm(vec1), norm(vec2)
	if norm1 == 0 && norm2 == 0 {
		return 0, nil
	}
	var dot float64
	for i := 0; i < len(vec1); i++ {
		dot += vec1[i] * vec2[i]
	}
	return dot / norm1 / norm2, nil
}
