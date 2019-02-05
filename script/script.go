// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package script implements scripting support for defining Diviner
// studies through Starlark [1]. This package enables the floating point
// and lambda extensions of the language.
//
// Script defines the following builtins for defining Diviner
// configurations: (Question marks indicate optional arguments.)
//
//	discrete(v1, v2, v3...)
//		Defines a discrete parameter that takes on the provided set
//		set of values (types string, float, or int).
//
//	range(beg, end)
//		Defines a range parameter with the given range.
//
//	minimize(metric)
//		Defines an objective that minimizes a metric (string).
//
//	maximize(metric)
//		Defines an objective that maximizes a metric (string).
//
//	dataset(name, if_not_exist?, local_files?, script)
//		Defines a dataset (diviner.Dataset):
//		- name:         the name of the dataset, which must be unique;
//		- if_not_exist: a URL that is checked for conditional execution;
//		- local_files:  a list of local files that must be made available
// 		                in the script's execution environment;
//		- script:       the script that is run to produce the dataset.
//
//	run_config(script, local_files?, datasets?)
//		Defines a run config (diviner.RunConfig) representing a single
//		trial:
//		- script:      the script that is executed for this trial;
//		- local_files: a list of local files that must be made available
//		               in the script's execution environment;
//		- datasets:    a list of datasets that must be available before
//		               the trial can proceed.
//
//	study(name, params, objective, run)
//		A toplevel function that declares a named study with the provided
//		parameters, runner, and objectives.
//		- name:      a string specifying the name of the study;
//		- objective: the optimization objective;
//		- params:    a dictionary with naming a set of parameters
//		             to be optimized;
//		- run:       a function that returns a run_config for a set
//		             of parameter values.
//
// Diviner configs must include one or more studies as toplevel declarations.
// Global starlark objects are frozen after initial evaluation to prevent functions
// from modifying shared state.
//
// [1] https://docs.bazel.build/versions/master/skylark/language.html
package script

import (
	"errors"
	"fmt"

	"github.com/grailbio/base/log"
	"github.com/grailbio/diviner"
	"go.starlark.net/resolve"
	"go.starlark.net/starlark"
)

func init() {
	resolve.AllowFloat = true
	resolve.AllowLambda = true
	// So that we can, e.g., define studies in a toplevel loop.
	resolve.AllowGlobalReassign = true
}

// Load loads configuration from a Starlark script. Arguments are as
// in Starlark's syntax.Parse: if src is not nil, it must be a byte
// source ([]byte or io.Reader); if src is nil, data are parsed from
// the provided filename.
//
// Load provides the builtins describes in the package documentation.
func Load(filename string, src interface{}) ([]diviner.Study, error) {
	thread := &starlark.Thread{
		Name:  "diviner",
		Print: func(_ *starlark.Thread, msg string) { log.Printf("%s: %s", filename, msg) },
	}
	var studies []diviner.Study
	thread.SetLocal("studies", &studies)
	builtins := starlark.StringDict{
		"discrete":   starlark.NewBuiltin("discrete", makeDiscrete),
		"range":      starlark.NewBuiltin("range", makeRange),
		"minimize":   starlark.NewBuiltin("minimize", makeObjective(diviner.Minimize)),
		"maximize":   starlark.NewBuiltin("maximize", makeObjective(diviner.Maximize)),
		"dataset":    starlark.NewBuiltin("dataset", makeDataset),
		"run_config": starlark.NewBuiltin("run_config", makeRunConfig),
		"study":      starlark.NewBuiltin("study", makeStudy),
	}
	globals, err := starlark.ExecFile(thread, filename, src, builtins)
	// Freeze everything now so that if we try to mutate global state
	// while invoking run configs later, we fail.
	for _, val := range globals {
		val.Freeze()
	}
	for _, study := range studies {
		study.Freeze()
	}
	return studies, err
}

func makeDiscrete(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	if len(kwargs) != 0 {
		return nil, errors.New("discrete does not accept any kwargs")
	}
	if len(args) == 0 {
		return nil, errors.New("discrete with empty list")
	}
	vals := make([]diviner.Value, len(args))
	for i, arg := range args {
		switch val := arg.(type) {
		case starlark.String:
			vals[i] = diviner.String(val.GoString())
		case starlark.Float:
			vals[i] = diviner.Float(float64(val))
		case starlark.Int:
			v64, ok := val.Int64()
			if !ok {
				return nil, fmt.Errorf("argument %s (%s) is not a valid diviner value", val, val.Type())
			}
			vals[i] = diviner.Int(v64)
		default:
			return nil, fmt.Errorf("argument %s (%s) is not a supported diviner value", val, val.Type())
		}
	}
	return diviner.NewDiscrete(vals...), nil
}

func makeRange(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	if len(kwargs) != 0 {
		return nil, errors.New("range does not accept any kwargs")
	}
	if len(args) != 2 {
		return nil, errors.New("range requires two arguments")
	}
	beg, ok := coerceToFloat(args[0])
	if !ok {
		return nil, fmt.Errorf("argument %s (%s) is not a valid diviner float", args[0], args[0].Type())
	}
	end, ok := coerceToFloat(args[1])
	if !ok {
		return nil, fmt.Errorf("argument %s (%s) is not a valid diviner float", args[1], args[1].Type())
	}
	return diviner.NewRange(beg, end), nil
}

func coerceToFloat(v starlark.Value) (float64, bool) {
	switch val := v.(type) {
	case starlark.Int:
		v64, ok := val.Int64()
		if !ok {
			return 0, false
		}
		return float64(v64), true
	case starlark.Float:
		return float64(val), true
	default:
		return 0, false
	}
}

func makeStudy(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var (
		study  diviner.Study
		params = new(starlark.Dict)
		runner = new(starlark.Function)
	)
	err := starlark.UnpackArgs(
		"study", args, kwargs,
		"name", &study.Name,
		"params", &params,
		"run", &runner,
		"objective", &study.Objective,
	)
	if err != nil {
		return nil, err
	}
	study.Params = make(diviner.Params)
	for _, tup := range params.Items() {
		keystr, ok := tup.Index(0).(starlark.String)
		if !ok {
			return nil, fmt.Errorf("parameter %s is not named by a string", tup.Index(0))
		}
		study.Params[string(keystr)], ok = tup.Index(1).(diviner.Param)
		if !ok {
			return nil, fmt.Errorf("parameter %s is not a valid parameter", string(keystr))
		}
	}
	study.Run = func(vals diviner.Values) diviner.RunConfig {
		var input starlark.Dict
		for key, value := range vals {
			var val starlark.Value
			switch value.(type) {
			case diviner.Float:
				val = starlark.Float(value.Float())
			case diviner.Int:
				val = starlark.MakeInt64(value.Int())
			case diviner.String:
				val = starlark.String(value.String())
			default:
				panic(value)
			}
			input.SetKey(starlark.String(key), val)
		}
		val, err := starlark.Call(thread, runner, starlark.Tuple{&input}, nil)
		if err != nil {
			// TODO(marius): allow run configs to fail
			panic(err)
		}
		config, ok := val.(diviner.RunConfig)
		if !ok {
			// TODO(marius): allow run configs to fail
			panic(config)
		}
		return config
	}
	studies := thread.Local("studies").(*[]diviner.Study)
	*studies = append(*studies, study)
	return study, err
}

func makeRunConfig(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var (
		config   diviner.RunConfig
		files    = new(starlark.List)
		datasets = new(starlark.List)
	)
	err := starlark.UnpackArgs(
		"run_config", args, kwargs,
		"script", &config.Script,
		"local_files?", &files,
		"datasets?", &datasets,
	)
	if err != nil {
		return nil, err
	}
	config.LocalFiles = make([]string, files.Len())
	for i := range config.LocalFiles {
		str, ok := files.Index(i).(starlark.String)
		if !ok {
			return nil, fmt.Errorf("file %s is not a string", files.Index(i))
		}
		config.LocalFiles[i] = string(str)
	}
	if datasets.Len() > 0 {
		config.Datasets = make([]diviner.Dataset, datasets.Len())
	}
	for i := range config.Datasets {
		var ok bool
		config.Datasets[i], ok = datasets.Index(i).(diviner.Dataset)
		if !ok {
			return nil, fmt.Errorf("dataset %s is not a dataset", datasets.Index(i))
		}
	}
	return config, nil
}

func makeDataset(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var (
		dataset diviner.Dataset
		files   = new(starlark.List)
	)
	err := starlark.UnpackArgs(
		"dataset", args, kwargs,
		"name", &dataset.Name,
		"if_not_exist?", &dataset.IfNotExist,
		"local_files?", &files,
		"script", &dataset.Script,
	)
	if err != nil {
		return nil, err
	}
	dataset.LocalFiles = make([]string, files.Len())
	for i := range dataset.LocalFiles {
		str, ok := files.Index(i).(starlark.String)
		if !ok {
			return nil, fmt.Errorf("file %s is not a string", files.Index(i))
		}
		dataset.LocalFiles[i] = string(str)
	}
	return dataset, nil
}

func makeObjective(direction diviner.Direction) func(*starlark.Thread, *starlark.Builtin, starlark.Tuple, []starlark.Tuple) (starlark.Value, error) {
	return func(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		o := diviner.Objective{Direction: direction}
		err := starlark.UnpackArgs(direction.String(), args, kwargs, "metric", &o.Metric)
		return o, err
	}
}
