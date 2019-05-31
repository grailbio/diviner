package oracle

import (
	"testing"

	"github.com/grailbio/diviner"
)

func TestRandom(t *testing.T) {
	const (
		cStart = 1.7
		cEnd   = 3.8
		dStart = 3
		dEnd   = 8
	)
	params := diviner.Params{
		"a": diviner.NewDiscrete(diviner.Float(0), diviner.Float(1), diviner.Float(2)),
		"b": diviner.NewDiscrete(diviner.Int(7), diviner.Int(8)),
		"c": diviner.NewRange(diviner.Float(cStart), diviner.Float(cEnd)),
		"d": diviner.NewRange(diviner.Int(dStart), diviner.Int(dEnd)),
	}

	random := NewRandom(0)
	const (
		numReps   = 1000
		maxPoints = 10
	)
	for i := 0; i < numReps; i++ {
		for j := 1; j <= maxPoints; j++ {
			values, err := random.Next(nil, params, diviner.Objective{}, j)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if len(values) != j {
				t.Fatalf("Wrong number of points returned: want %d; got %d", j, len(values))
			}
			for k := 0; k < j; k++ {
				if !params.IsValid(values[k]) {
					t.Errorf("Sample returned invalid parameter values: %v", values)
				}
			}
		}
	}
}
