package oracle

import (
	"encoding/gob"
	"math/rand"
	"sync"

	"github.com/grailbio/diviner"
)

func init() {
	gob.Register(&Random{})
}

// Random is an oracle that returns random points in the search space. This is typically much more
// effective than grid search for most hyperparameter optimization problems.
// Currently, this oracle supports only integer and real parameter types.
type Random struct {
	// Seed records the random seed that will be used to initialize random number generation for
	// the next point. It is exported so it can be serialized to preserve the oracle's state.
	Seed  int64
	mutex sync.Mutex
}

// NewRandom returns a new Random oracle with the given random seed.
func NewRandom(seed int64) *Random {
	return &Random{
		Seed: seed,
	}
}

// Next implements Oracle.Next.
func (r *Random) Next(previous []diviner.Trial, params diviner.Params, objective diviner.Objective,
	howmany int) ([]diviner.Values, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	result := make([]diviner.Values, howmany)
	for i := range result {
		result[i] = r.nextPoint(params)
	}
	return result, nil
}

func (r *Random) nextPoint(params diviner.Params) diviner.Values {
	random := rand.New(rand.NewSource(r.Seed))
	r.Seed++
	result := make(diviner.Values)
	for _, param := range params.Sorted() {
		result[param.Name] = param.Sample(random)
	}
	return result
}
