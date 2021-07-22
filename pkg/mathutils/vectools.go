package mathutils

// VecMean creates a mean/average vec from a vector generator.
func VecMean(generator func() ([]float64, bool)) ([]float64, bool) {
	vec, cont := generator()
	if !cont {
		return vec, false
	}

	res := make([]float64, len(vec))
	copy(res, vec)

	n := 1.
	for {
		vec, cont := generator()
		if !cont {
			break
		}
		if len(vec) != len(res) {
			return res, false
		}
		for i := 0; i < len(res); i++ {
			res[i] += vec[i]
		}
		n += 1
	}

	for i := 0; i < len(res); i++ {
		res[i] /= n
	}

	return res, true
}

// Vec conveniently creates a vector.
func Vec(v ...float64) []float64 {
	_vec := make([]float64, len(v))
	for i, x := range v {
		_vec[i] = x
	}
	return _vec
}

// VecEq checks if two vectors are equal.
func VecEq(v1, v2 []float64) bool {
	if len(v1) != len(v2) {
		return false
	}
	for i := 0; i < len(v1); i++ {
		if v1[i] != v2[i] {
			return false
		}
	}
	return true
}

// VecIn checks if a vector is in a slice of vectors.
func VecIn(vec []float64, vecs [][]float64) bool {
	for _, other := range vecs {
		if VecEq(vec, other) {
			return true
		}
	}
	return false
}

// VecDivScalar does a scalar division on a vector.
// Zero div is not checked.
func VecDivScalar(vec []float64, scalar float64) []float64 {
	res := make([]float64, len(vec))
	for i, v := range vec {
		res[i] = v / scalar
	}
	return res
}

// VecMulScalar does a scalar multiplication on a vector.
func VecMulScalar(vec []float64, scalar float64) []float64 {
	res := make([]float64, len(vec))
	for i, v := range vec {
		res[i] = v * scalar
	}
	return res
}

// VecAdd adds two vectors.
func VecAdd(a, b []float64) ([]float64, bool) {
	if len(a) != len(b) {
		return nil, false
	}
	res := make([]float64, len(a))
	for i := 0; i < len(a); i++ {
		res[i] = a[i] + b[i]
	}
	return res, true
}

// VecSub subtracts two vectors (b from a).
func VecSub(a, b []float64) ([]float64, bool) {
	if len(a) != len(b) {
		return nil, false
	}
	res := make([]float64, len(a))
	for i := 0; i < len(a); i++ {
		res[i] = a[i] - b[i]
	}
	return res, true
}
