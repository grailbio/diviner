// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package script implements scripting support for defining Diviner
// studies through Starlark [1]. This package enables the floating point
// and lambda extensions of the language.
//
// Script defines the following builtins for defining Diviner
// configurations (question marks indicate optional arguments):
//
//	config(database, table)
//		Configures Diviner to use the provided database
//		("local" or "dynamodb") and the given table
//		(local filename or dynamodb table name) to store
// 		all study state.
//
//	discrete(v1, v2, v3...)
//		Defines a discrete parameter that takes on the provided set
//		set of values (types string, float, or int).
//
//	range(beg, end)
//		Defines a range parameter with the given range. (Integers or floats.)
//
//	minimize(metric)
//		Defines an objective that minimizes a metric (string).
//
//	maximize(metric)
//		Defines an objective that maximizes a metric (string).
//
//	localsystem(name, parallelism)
//		Defines a new local system with the provided name and parallelism.
//		The provided name is used to identify the system in tools.
//
//	ec2system(name, parallelism, ami, instance_profile, instance_type, disk_space?, data_space?, on_demand?, flavor?)
//		Defines a new EC2-based system of the given name, parallelism, and
//		configuration. The provided name is used to identify the system in tools.
//		- ami:              the EC2 AMI to use when launching new instances;
//		- instance_profile: the IAM instance profile assigned to new instances;
//		- instance_type:    the instance type used;
//		- disk_space:       the amount of root disk space created;
//		- data_space:       the amount of data/scratch space created;
//		- on_demand:        (bool) whether to launch on-demand instance types;
//		- flavor:           the flavor of AMI: "ubuntu" or "coreos".
//		See package github.com/grailbio/bigmachine/ec2system for more details on these
//		parameters.
//
//	dataset(name, system, if_not_exist?, local_files?, script)
//		Defines a dataset (diviner.Dataset):
//		- name:         the name of the dataset, which must be unique;
//		- system:       the system to be used for dataset execution;
//		- if_not_exist: a URL that is checked for conditional execution;
//		- local_files:  a list of local files that must be made available
// 		                in the script's execution environment;
//		- script:       the script that is run to produce the dataset.
//
//	run_config(script, system, local_files?, datasets?)
//		Defines a run config (diviner.RunConfig) representing a single
//		trial:
//		- script:      the script that is executed for this trial;
//		- system:      the system to be used for run execution;
//		- local_files: a list of local files that must be made available
//		               in the script's execution environment;
//		- datasets:    a list of datasets that must be available before
//		               the trial can proceed.
//
//	study(name, params, objective, run, oracle?)
//		A toplevel function that declares a named study with the provided
//		parameters, runner, and objectives.
//		- name:      a string specifying the name of the study;
//		- objective: the optimization objective;
//		- params:    a dictionary with naming a set of parameters
//		             to be optimized;
//		- run:       a function that returns a run_config for a set
//		             of parameter values; the first argument to the function
//		             is a dictionary of parameter values.
//		- oracle:    the oracle to use (grid search by default).
//
//	grid_search
//		The grid search oracle
//
//	skopt(base_estimator?, n_initial_points?, acq_func?, acq_optimizer?)
//		A Bayesian optimization oracle based on skopt. The arguments
//		are as in skopt.Optimizer, documented at
//		https://scikit-optimize.github.io/optimizer/index.html#skopt.optimizer.Optimizer:
//		- base_estimator:   the base estimator to be used, one of
//		                    "GP", "RF", "ET", "GBRT" (default "GP");
//		- n_initial_points: number of evaluations to perform before estimating
//		                    using the above estimator (default 10);
//		- acq_func:         the acquisition function to use for sampling
//		                    new points, one of "LCB", "EI", "PI", or
//		                    "gp_hedge" (default "gp_hedge");
//		- acq_optimizer:    the optimizer used to minimize the acquisitino function,
//		                    one of "sampling", "lgbfs" (by default it is automatically
//		                    selected).
//
//  command(script, interpreter?="bash -c")
//    Run a subprocess and return its standard output as a string.
//    - script: the script to run; a string.
//    - interpreter: command that runs the script. It defaults to "bash -c".
//
//    For example, command("print('foo'*2)", interpreter="python3 -c") will produce
//    "foofoo\n".
//
// Diviner configs must include one or more studies as toplevel declarations.
// Global starlark objects are frozen after initial evaluation to prevent functions
// from modifying shared state.
//
// [1] https://docs.bazel.build/versions/master/skylark/language.html
package script

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/grailbio/base/log"
	"github.com/grailbio/bigmachine"
	"github.com/grailbio/bigmachine/ec2system"
	"github.com/grailbio/diviner"
	"github.com/grailbio/diviner/oracle"
	"go.starlark.net/resolve"
	"go.starlark.net/starlark"
)

func init() {
	resolve.AllowFloat = true
	resolve.AllowLambda = true
	// So that we can, e.g., define studies in a toplevel loop.
	resolve.AllowGlobalReassign = true
	resolve.AllowRecursion = true
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
		"discrete":    starlark.NewBuiltin("discrete", makeDiscrete),
		"range":       starlark.NewBuiltin("range", makeRange),
		"minimize":    starlark.NewBuiltin("minimize", makeObjective(diviner.Minimize)),
		"maximize":    starlark.NewBuiltin("maximize", makeObjective(diviner.Maximize)),
		"dataset":     starlark.NewBuiltin("dataset", makeDataset),
		"run_config":  starlark.NewBuiltin("run_config", makeRunConfig),
		"study":       starlark.NewBuiltin("study", makeStudy),
		"grid_search": &oracleValue{&oracle.GridSearch{}},
		"skopt":       starlark.NewBuiltin("skopt", makeSkopt),
		"config":      starlark.NewBuiltin("config", makeConfig),
		"localsystem": starlark.NewBuiltin("localsystem", makeLocalSystem),
		"ec2system":   starlark.NewBuiltin("ec2system", makeEC2System),
		"command":     starlark.NewBuiltin("command", makeCommand),
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

type oracleValue struct{ diviner.Oracle }

func (o *oracleValue) String() string { return fmt.Sprint(o.Oracle) }

func (*oracleValue) Type() string { return "oracle" }

func (*oracleValue) Freeze() {}

func (*oracleValue) Truth() starlark.Bool { return true }

func (*oracleValue) Hash() (uint32, error) { return 0, errors.New("oracles not hashable") }

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
	switch beg := args[0].(type) {
	case starlark.Int:
		beg64, ok := beg.Int64()
		if !ok {
			return nil, fmt.Errorf("argument %s overflows int64", beg)
		}
		end, ok := args[1].(starlark.Int)
		if !ok {
			return nil, fmt.Errorf("argument mismatch: %s is int, %s is %s", beg, args[1], args[1].Type())
		}
		end64, ok := end.Int64()
		if !ok {
			return nil, fmt.Errorf("argument %s overflows int64", end)
		}
		return diviner.NewRange(diviner.Int(beg64), diviner.Int(end64)), nil
	case starlark.Float:
		end, ok := args[1].(starlark.Float)
		if !ok {
			return nil, fmt.Errorf("argument mismatch: %s is float, %s is %s", beg, args[1], args[1].Type())
		}
		return diviner.NewRange(diviner.Float(beg), diviner.Float(end)), nil
	default:
		return nil, fmt.Errorf("arguments %s, %s (types %s, %s) invalid for range", args[0], args[1], args[0].Type(), args[1].Type())
	}
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
		oracle = new(oracleValue)
		params = new(starlark.Dict)
		runner = new(starlark.Function)
	)
	err := starlark.UnpackArgs(
		"study", args, kwargs,
		"name", &study.Name,
		"params", &params,
		"run", &runner,
		"objective", &study.Objective,
		"oracle?", &oracle,
	)
	if err != nil {
		return nil, err
	}
	study.Oracle = oracle.Oracle
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
	study.Run = func(vals diviner.Values) (diviner.RunConfig, error) {
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
		thread := &starlark.Thread{
			Name: "diviner",
		}
		val, err := starlark.Call(thread, runner, starlark.Tuple{&input}, nil)
		if err != nil {
			return diviner.RunConfig{}, err
		}
		config, ok := val.(diviner.RunConfig)
		if !ok {
			return diviner.RunConfig{}, err
		}
		return config, nil
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
		"system", &config.System,
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
		"system", &dataset.System,
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

func makeSkopt(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	skopt := new(oracle.Skopt)
	return &oracleValue{skopt}, starlark.UnpackArgs(
		"skopt", args, kwargs,
		"base_estimator?", &skopt.BaseEstimator,
		"n_initial_points?", &skopt.NumInitialPoints,
		"acq_func?", &skopt.AcquisitionFunc,
		"acq_optimizer?", &skopt.AcquisitionOptimizer,
	)
}

func makeConfig(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	log.Error.Printf("%s: config is deprecated and will be ignored", thread.Caller().Position())
	return starlark.None, nil
}

func makeLocalSystem(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	system := &diviner.System{System: bigmachine.Local}
	err := starlark.UnpackArgs(
		"localsystem", args, kwargs,
		"name", &system.ID,
		"parallelism", &system.Parallelism,
	)
	return system, err
}

func makeEC2System(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var (
		system               = new(diviner.System)
		ec2                  = new(ec2system.System)
		flavor               string
		diskspace, dataspace int // UnpackArgs doesn't support uint
	)
	system.System = ec2
	err := starlark.UnpackArgs(
		"ec2system", args, kwargs,
		"name", &system.ID,
		"parallelism", &system.Parallelism,
		"ami", &ec2.AMI,
		"instance_profile", &ec2.InstanceProfile,
		"instance_type", &ec2.InstanceType,
		"disk_space?", &diskspace,
		"data_space?", &dataspace,
		"on_demand?", &ec2.OnDemand,
		"flavor?", &flavor,
	)
	if err != nil {
		return nil, err
	}
	switch flavor {
	case "", "ubuntu":
		ec2.Flavor = ec2system.Ubuntu
	case "coreos":
		ec2.Flavor = ec2system.CoreOS
	default:
		return nil, fmt.Errorf("invalid AMI flavor %s", flavor)
	}
	if diskspace < 0 {
		return nil, errors.New("negative disk_space not allowed")
	}
	if dataspace < 0 {
		return nil, errors.New("negative data_space not allowed")
	}
	ec2.Diskspace = uint(diskspace)
	ec2.Dataspace = uint(dataspace)
	return system, nil
}

func makeCommand(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var (
		script      string
		interpreter = "bash -c"
	)
	err := starlark.UnpackArgs(
		"command", args, kwargs,
		"script", &script,
		"interpreter?", &interpreter,
	)
	if err != nil {
		return nil, err
	}
	outbuf := bytes.Buffer{}
	cmd := exec.Cmd{
		Args:   append(strings.Split(interpreter, " "), script),
		Stdout: &outbuf,
		Stderr: os.Stderr,
	}
	if cmd.Path, err = exec.LookPath(cmd.Args[0]); err != nil {
		return nil, err
	}
	if err := cmd.Run(); err != nil {
		return nil, err
	}
	return starlark.String(outbuf.Bytes()), nil
}
