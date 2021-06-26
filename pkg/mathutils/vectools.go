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
