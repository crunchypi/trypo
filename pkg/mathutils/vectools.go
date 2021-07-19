package mathutils

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

func VecDivScalar(vec []float64, scalar float64) []float64 {
	res := make([]float64, len(vec))
	for i, v := range vec {
		res[i] = v / scalar
	}
	return res
}

func VecMulScalar(vec []float64, scalar float64) []float64 {
	res := make([]float64, len(vec))
	for i, v := range vec {
		res[i] = v * scalar
	}
	return res
}

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
