// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package oracle

import (
	"bytes"
	"encoding/csv"
	"encoding/gob"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"text/template"

	"github.com/grailbio/diviner"
)

func init() {
	gob.Register(&Skopt{})
}

// TODO(marius): expand the oracle interface to check if the oracle is available
// (e.g., in this case, we may not have python or skopt installed)

// TODO(marius): set random_state in the skopt optimizer for determinism;
// store this state somewhere.

var skoptTemplate = template.Must(template.New("skopt").Parse(`
import skopt
opt = skopt.Optimizer([{{.params}}] {{.kwargs}})
xs = [{{.xs}}]
ys = [{{.ys}}]
if len(xs) > 0:
    opt.tell(xs, ys)
points = opt.ask(n_points={{.n}}, strategy="{{.strategy}}")
for point in points:
    print("\t".join(map(str, point)))
`))

// Skopt is an oracle that uses scikit-optimize [1] to perform
// bayesian optimization. It works by calling into Python, and
// expects the environment to have a Python installation with
// scikit-optimize available. (It may be installed with "pip install
// scikit-optimize"). The parameters in the class are passed to
// skopt.Optimizer, which is documented at [2]. Default values
// are used for parameter's zero values, also defined at [2].
//
// [1] https://scikit-optimize.github.io/
// [2] https://scikit-optimize.github.io/optimizer/index.html#skopt.optimizer.Optimizer
//
// TODO(marius): support passing in scikit-optimize's "random state"
// for determinism.
//
// TODO(marius): figure out a better distribution mechanism, perhaps by
// bundling a PEX package or the like.
type Skopt struct {
	// BaseEstimator is the estimator used in by the oracle. It is one
	// of "GP" (default), "RF", "ET", "GBRT"; documented at
	// https://scikit-optimize.github.io/optimizer/index.html#skopt.optimizer.Optimizer
	BaseEstimator string
	// NumInitialPoints specifies the number of initialization
	// evaluations to perform before approximating the function using
	// the above estimator.
	NumInitialPoints int
	// AcquisitionFunc is the acquisition function that is used. One of
	// "gp_hedge" (default), "LCB", "EI", or "PI". See
	// https://scikit-optimize.github.io/optimizer/index.html#skopt.optimizer.Optimizer
	// for more details.
	AcquisitionFunc string
	// AcquisitionOptimizer is the method by which the acquisition
	// function is minimized. It is one of "sampling" or "lgbfs". By
	// default, the optimizer is selected automatically based on the
	// estimator and parameter space.
	AcquisitionOptimizer string
}

// Next performs a single round of optimizations, yielding n trials.
// It fits a model based on the datapoints provided by the trials
// using the provided objective. This model is then used to select
// the next set of points by optimizing the configured acquisition
// function.
func (s *Skopt) Next(trials []diviner.Trial, params diviner.Params, objective diviner.Objective, n int) ([]diviner.Values, error) {
	// First construct skopt spaces from diviner params.
	sortedParams := params.Sorted()
	skoptParams := make([]string, len(sortedParams))
	for i, param := range sortedParams {
		switch p := param.Param.(type) {
		case *diviner.Range:
			switch p.Kind() {
			case diviner.Integer:
				skoptParams[i] = fmt.Sprintf("skopt.space.Integer(%d, %d)", p.Start.Int(), p.End.Int())
			case diviner.Real:
				skoptParams[i] = fmt.Sprintf("skopt.space.Real(%f, %f)", p.Start.Float(), p.End.Float())
			default:
				panic(p)
			}
		case *diviner.Discrete:
			values := p.Values()
			categories := make([]string, len(values))
			for i, v := range values {
				categories[i] = fmt.Sprintf("%q", v.String())
			}
			skoptParams[i] = fmt.Sprintf("skopt.space.Categorical([%s])", strings.Join(categories, ", "))
		default:
			panic(p)
		}
	}

	// Map trails into datapoints vis-a-vis the above skopt spcaes.
	var (
		xs = make([]string, len(trials))
		ys = make([]string, len(trials))
	)
	for i, trial := range trials {
		x := make([]string, len(sortedParams))
		for i, param := range sortedParams {
			val := trial.Values[param.Name]
			switch param.Param.(type) {
			case *diviner.Range:
				x[i] = val.String()
			case *diviner.Discrete:
				x[i] = fmt.Sprintf("%q", val)
			}
		}
		xs[i] = fmt.Sprintf("[%s]", strings.Join(x, ", "))
		ys[i] = fmt.Sprint(trial.Metrics[objective.Metric])
	}

	// Construct the estimator based on struct parameters and
	// the objective.
	var strategy string
	switch objective.Direction {
	case diviner.Minimize:
		strategy = "cl_min"
	case diviner.Maximize:
		strategy = "cl_max"
	default:
		panic(objective.Direction)
	}
	var kwargs string
	if s.BaseEstimator != "" {
		kwargf(&kwargs, "base_estimator", "%q", s.BaseEstimator)
	}
	if s.NumInitialPoints > 0 {
		kwargf(&kwargs, "n_initial_points", "%d", s.NumInitialPoints)
	}
	if s.AcquisitionFunc != "" {
		kwargf(&kwargs, "acq_func", "%q", s.AcquisitionFunc)
	}
	if s.AcquisitionOptimizer != "" {
		kwargf(&kwargs, "acq_optimizer", "%q", s.AcquisitionOptimizer)
	}
	var script bytes.Buffer
	err := skoptTemplate.Execute(&script, map[string]interface{}{
		"params":   strings.Join(skoptParams, ", "),
		"kwargs":   kwargs,
		"xs":       strings.Join(xs, ", "),
		"ys":       strings.Join(ys, ", "),
		"strategy": strategy,
		"n":        n,
	})
	if err != nil {
		return nil, err
	}

	// Finally perform the actual optimization by renderering a python
	// script that invokes the optimizer and prints the sampled points
	// to stdout in TSV format, which we in turn parse into diviner values.
	var out bytes.Buffer
	cmd := exec.Command("python")
	cmd.Stdin = &script
	cmd.Stdout = &out
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return nil, err
	}
	r := csv.NewReader(&out)
	r.Comma = '\t'
	records, err := r.ReadAll()
	if err != nil {
		return nil, err
	}
	values := make([]diviner.Values, len(records))
	for i, record := range records {
		if len(record) != len(sortedParams) {
			panic(record)
		}
		vals := make(diviner.Values)
		for j, param := range sortedParams {
			var (
				val diviner.Value
				str = record[j]
			)
			switch param.Kind() {
			case diviner.Integer:
				v64, err := strconv.ParseInt(str, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("invalid integer value %s: %v", str, err)
				}
				val = diviner.Int(v64)
			case diviner.Real:
				v64, err := strconv.ParseFloat(record[j], 64)
				if err != nil {
					return nil, fmt.Errorf("invalid float value %s: %v", str, err)
				}
				val = diviner.Float(v64)
			case diviner.Str:
				str, err := url.QueryUnescape(record[j])
				if err != nil {
					return nil, fmt.Errorf("invalid string value %s: %v", str, err)
				}
				val = diviner.String(str)
			}
			vals[sortedParams[j].Name] = val
		}
		values[i] = vals
	}
	return values, nil
}

func kwargf(p *string, k, format string, v ...interface{}) {
	*p = fmt.Sprintf("%s, %s=%s", *p, k, fmt.Sprintf(format, v...))
}
