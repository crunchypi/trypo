package searchutils

import (
	"fmt"
	"testing"
)

func TestBubble(t *testing.T) {
	insertee := resultItem{3, 3, false}
	res := []resultItem{
		{2, 2, false},
		{1, 1, false},
		{0, 0, false},
	}
	bubble(&insertee, res, false)
	if res[0].index != 3 || res[1].index != 2 || res[2].index != 1 {
		t.Errorf("unordered on bubble up: %v", res)
	}
	insertee = resultItem{0, 0, false}
	res = []resultItem{
		{1, 1, false},
		{2, 2, false},
		{3, 3, false},
	}
	bubble(&insertee, res, true)
	if res[0].index != 0 || res[1].index != 1 || res[2].index != 2 {
		t.Errorf("unordered on bubble down: %v", res)
	}
}

func TestKNNCos(t *testing.T) {
	targetVec := []float64{1, 1, 1}
	vecPool := [][]float64{
		// Increasingly crooked angles.
		{1, 2, 3}, // Interval 1.
		{1, 3, 5}, // Interval 2.
		{1, 4, 8}, // Interval 3.
	}
	i := 0
	generator := func() ([]float64, bool) {
		if i == len(vecPool) {
			return nil, false
		}
		i++
		return vecPool[i-1], true
	}
	res := KNNCos(targetVec, generator, 2)
	if res[0] != 0 || res[1] != 1 {
		t.Errorf("Cosine simi not in correct order: %v", res)
	}
	fmt.Println(res)
}
func TestKFNCos(t *testing.T) {
	targetVec := []float64{1, 1, 1}
	vecPool := [][]float64{
		// Increasingly crooked angles.
		{1, 2, 3}, // Interval 1.
		{1, 3, 5}, // Interval 2.
		{1, 4, 8}, // Interval 3.
	}
	i := 0
	generator := func() ([]float64, bool) {
		if i == len(vecPool) {
			return nil, false
		}
		i++
		return vecPool[i-1], true
	}
	res := KFNCos(targetVec, generator, 2)
	if res[0] != 2 || res[1] != 1 {
		t.Errorf("Cosine simi not in correct order: %v", res)
	}
	fmt.Println(res)
}

func TestKNNEuc(t *testing.T) {

	targetVec1 := []float64{5, 5, 5}
	vecPool := [][]float64{
		// Increasingly from a 3d point.
		{2, 2, 2},
		{3, 3, 3},
		{4, 4, 4},
	}
	i := 0
	generator := func() ([]float64, bool) {
		if i == len(vecPool) {
			return nil, false
		}
		i++
		return vecPool[i-1], true
	}
	res := KNNEuc(targetVec1, generator, 2)
	t.Log(res)
	if res[0] != 2 || res[1] != 1 {
		t.Errorf("1) Euclidean simi not in correct order: %v", res)
	}
	i = 0
	targetVec2 := []float64{5, 5, 5}
	res = KNNEuc(targetVec2, generator, 2)
	t.Log(res)
	if res[0] != 2 || res[1] != 1 {
		t.Errorf("2) Euclidean simi not in correct order: %v", res)
	}

}
func TestKFNEuc(t *testing.T) {

	targetVec1 := []float64{5, 5, 5}
	vecPool := [][]float64{
		// Increasingly from a 3d point.
		{2, 2, 2},
		{3, 3, 3},
		{4, 4, 4},
	}
	i := 0
	generator := func() ([]float64, bool) {
		if i == len(vecPool) {
			return nil, false
		}
		i++
		return vecPool[i-1], true
	}
	res := KFNEuc(targetVec1, generator, 2)
	t.Log(res)
	if res[0] != 0 || res[1] != 1 {
		t.Errorf("1) Euclidean simi not in correct order: %v", res)
	}
	i = 0
	targetVec2 := []float64{5, 5, 5}
	res = KFNEuc(targetVec2, generator, 2)
	t.Log(res)
	if res[0] != 0 || res[1] != 1 {
		t.Errorf("2) Euclidean simi not in correct order: %v", res)
	}

}
