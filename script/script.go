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
//	localsystem(name, parallelism?)
//		Defines a new local system with the provided name.  The name is used to
//		identify the system in tools.  The parallelism limits the number of jobs
//		that run on this system simultaneously.  If parallelism is unset, it
//		defaults to ∞.
//
//	ec2system(name, ami, instance_profile, instance_type, disk_space?, data_space?, on_demand?, flavor?)
//		Defines a new EC2-based system of the given name, and configuration.
//		The provided name is used to identify the system in tools.
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
//		- system:       the system(s) to be used for run execution. The value is either
//                    a single system or a list of systems. In the latter case,
//                    the run will use any one of systems can allocate resources.
//		- if_not_exist: a URL that is checked for conditional execution;
//		                dataset invocations are de-duped based on this URL.
//		- local_files:  a list of local files that must be made available
// 		                in the script's execution environment;
//		- script:       the script that is run to produce the dataset.
//
//	run_config(script, system, local_files?, datasets?)
//		Defines a run config (diviner.RunConfig) representing a single
//		trial:
//		- script:      the script that is executed for this trial;
//		- system:      the system(s) to be used for run execution. The value is either
//                   a single system or a list of systems. In the latter case,
//                   the run will use any one of systems can allocate resources.
//		- local_files: a list of local files that must be made available
//		               in the script's execution environment;
//		- datasets:    a list of datasets that must be available before
//		               the trial can proceed.
//
//	study(name, params, objective, run, replicates?, oracle?)
//		A toplevel function that declares a named study with the provided
//		parameters, runner, and objectives.
//		- name:       a string specifying the name of the study;
//		- objective:  the optimization objective;
//		- params:     a dictionary with naming a set of parameters
//		              to be optimized;
//		- run:        a function that returns a run_config for a set
//		              of parameter values; the first argument to the function
//		              is a dictionary of parameter values. A number of optional,
//		              named arguments follow: "id" is a string providing the
//		              run's diviner ID, which may be used as an external key to
//		              reference a particular run; "replicate" is an integer
//		              specifying the replicate number associated with the run.
//		- replicates: the number of replicates to perform for each parameter
// 		              combination.
//		- oracle:     the oracle to use (grid search by default).
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
//  	command(script, interpreter?="bash -c", strip?=False)
//		Run a subprocess and return its standard output as a string.
//		- script: the script to run; a string.
//		- interpreter: command that runs the script. It defaults to "bash -c".
//		- strip: strip leading and training whitespace from the command's output.
//
// 	For example, command("print('foo'*2)", interpreter="python3 -c") will produce
//	"foofoo\n".
//
//	temp_file(contents)
//		Create a temporary file from the provided contents (a string), and return
//		its path.
//
//	enum_value(str)
//		Internal representation of a protocol buffer enumeration value.
//		(See to_proto).
//
//	to_proto(dict):
//		Render a string-keyed dictionary to the text protocol buffer format.
//		Dictionaries cannot currently be nested. Enumeration values as created
//		by enum_value are rendered as protocol buffer enumeration, not strings.
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
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
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
	resolve.AllowNestedDef = true
}

var builtins = starlark.StringDict{
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
	"temp_file":   starlark.NewBuiltin("temp_file", makeTempFile),
	"enum_value":  starlark.NewBuiltin("enum_value", makeEnumValue),
	"to_proto":    starlark.NewBuiltin("to_proto", makeToProto),
}

func makeLoader(entrypoint string) func(thread *starlark.Thread, module string) (starlark.StringDict, error) {
	type loadEntry struct {
		globals starlark.StringDict
		err     error
	}
	var (
		cache   = make(map[string]*loadEntry)
		basedir = filepath.Dir(entrypoint)
	)
	// Prevent cycles involving the main module.
	cache[entrypoint] = nil

	return func(thread *starlark.Thread, module string) (starlark.StringDict, error) {
		var filename string
		if caller := thread.Caller(); caller != nil {
			filename = filepath.Join(filepath.Dir(thread.Caller().Position().Filename()), module)
		} else {
			filename = filepath.Join(basedir, module)
		}
		if e, ok := cache[filename]; ok {
			if e == nil {
				return nil, fmt.Errorf("cycle in load graph involving module %s", filename)
			}
			return e.globals, e.err
		}
		cache[filename] = nil
		var e loadEntry
		e.globals, e.err = starlark.ExecFile(thread, filename, nil, builtins)
		cache[filename] = &e
		return e.globals, e.err
	}
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
		Load:  makeLoader(filename),
	}
	var studies []diviner.Study
	thread.SetLocal("studies", &studies)
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
		vals[i] = starlark2diviner(arg)
		if vals[i] == nil {
			return nil, fmt.Errorf("argument %s (%s) is not a valid diviner value", arg, arg.Type())
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
		"replicates?", &study.Replicates,
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
	for i := 1; i < runner.NumParams(); i++ {
		switch name, _ := runner.Param(i); name {
		default:
			return nil, fmt.Errorf("illegal parameter name %s in run function", name)
		case "id", "replicate":
		}
	}
	study.Run = func(vals diviner.Values, replicate int, runID string) (diviner.RunConfig, error) {
		var input starlark.Dict
		for key, value := range vals {
			val := diviner2starlark(value)
			if val == nil {
				panic(fmt.Sprintf("%T", value))
			}
			input.SetKey(starlark.String(key), val)
		}
		thread := &starlark.Thread{
			Name: "diviner",
		}
		args := make(starlark.Tuple, runner.NumParams())
		args[0] = &input
		for i := 1; i < runner.NumParams(); i++ {
			switch name, _ := runner.Param(i); name {
			default:
				panic(name)
			case "id":
				args[i] = starlark.String(runID)
			case "replicate":
				args[i] = starlark.MakeInt(replicate)
			}
		}
		val, err := starlark.Call(thread, runner, args, nil)
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

func extractSystems(val starlark.Value) (systems []*diviner.System, err error) {
	switch s := val.(type) {
	case *diviner.System:
		return []*diviner.System{s}, nil
	case *starlark.List:
		n := s.Len()
		for i := 0; i < n; i++ {
			system, ok := s.Index(i).(*diviner.System)
			if !ok {
				return nil, fmt.Errorf("Element #%d of 'systems' arg must be a system, but found %v", i, s.Index(i))
			}
			systems = append(systems, system)
		}
		return systems, nil
	default:
		return nil, fmt.Errorf("'systems' arg must be a list or an instance of a system, but found %v", val)
	}
}

func makeRunConfig(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var (
		config   diviner.RunConfig
		files    = new(starlark.List)
		datasets = new(starlark.List)
		systems  = new(starlark.Value)
	)
	err := starlark.UnpackArgs(
		"run_config", args, kwargs,
		"system", systems,
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
	if config.Systems, err = extractSystems(*systems); err != nil {
		return nil, err
	}
	return config, nil
}

func makeDataset(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var (
		dataset diviner.Dataset
		files   = new(starlark.List)
		systems = new(starlark.Value)
	)
	err := starlark.UnpackArgs(
		"dataset", args, kwargs,
		"name", &dataset.Name,
		"system", systems,
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
	if dataset.Systems, err = extractSystems(*systems); err != nil {
		return nil, err
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
		"parallelism?", &system.Parallelism,
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
		"ami", &ec2.AMI,
		"region?", &ec2.Region,
		"security_group?", &ec2.SecurityGroup,
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
		strip       bool
	)
	err := starlark.UnpackArgs(
		"command", args, kwargs,
		"script", &script,
		"interpreter?", &interpreter,
		"strip?", &strip,
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
	log.Debug.Printf("command: %s", strings.Join(cmd.Args, " "))
	if cmd.Path, err = exec.LookPath(cmd.Args[0]); err != nil {
		return nil, err
	}
	if err := cmd.Run(); err != nil {
		return nil, err
	}
	out := outbuf.Bytes()
	if strip {
		out = bytes.TrimSpace(out)
	}
	return starlark.String(out), nil
}

func makeTempFile(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var contents string
	err := starlark.UnpackArgs(
		"temp_file", args, kwargs,
		"contents", &contents,
	)
	if err != nil {
		return nil, err
	}
	f, err := ioutil.TempFile("", "")
	if err != nil {
		return nil, err
	}
	if _, err := io.WriteString(f, contents); err != nil {
		return nil, err
	}
	name := f.Name()
	if err := f.Close(); err != nil {
		return nil, err
	}
	return starlark.String(name), nil
}

type enumValue string

func makeEnumValue(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var str string
	err := starlark.UnpackArgs(
		"enum", args, kwargs,
		"value", &str,
	)
	if err != nil {
		return nil, err
	}
	return enumValue(str), nil
}

func (v enumValue) String() string     { return string(v) }
func (v enumValue) Type() string       { return "proto_enum" }
func (enumValue) Freeze()              {}
func (enumValue) Truth() starlark.Bool { return true }
func (v enumValue) Hash() (uint32, error) {
	return starlark.String(v).Hash()
}

func dictKey(v starlark.Value) (string, error) {
	keyValue, ok := v.(starlark.String)
	if !ok {
		return "", fmt.Errorf("invalid key %s: keys must be string typed, not %s", v.String(), v.Type())
	}
	return string(keyValue), nil
}

func valueToProto(buf *strings.Builder, v starlark.Value, indent string) error {
	switch val := v.(type) {
	case *starlark.List:
		panic(v)
	case *starlark.Dict:
		buf.WriteString(indent + "{\n")
		for _, kv := range val.Items() {
			subkey, err := dictKey(kv[0])
			if err != nil {
				return err
			}
			if err := keyValueToProto(buf, subkey, kv[1], indent+"  "); err != nil {
				return err
			}
		}
		buf.WriteString("}\n")
	default:
		fmt.Fprintf(buf, "%v", val)
	}
	return nil
}

func keyValueToProto(buf *strings.Builder, key string, v starlark.Value, indent string) error {
	switch val := v.(type) {
	case *starlark.List:
		for i := 0; i < val.Len(); i++ {
			if err := keyValueToProto(buf, key, val.Index(i), indent); err != nil {
				return err
			}
		}
	case *starlark.Dict:
		buf.WriteString(indent + key + " {\n")
		for _, kv := range val.Items() {
			subkey, err := dictKey(kv[0])
			if err != nil {
				return err
			}
			if err := keyValueToProto(buf, subkey, kv[1], indent+"  "); err != nil {
				return err
			}
		}
		buf.WriteString(indent + "}\n")
	default:
		buf.WriteString(indent + key + ": ")
		if err := valueToProto(buf, v, indent); err != nil {
			return err
		}
		buf.WriteString("\n")
	}
	return nil
}

func makeToProto(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var (
		dict  = new(starlark.Dict)
		enums []string
	)
	err := starlark.UnpackArgs(
		"to_proto", args, kwargs,
		"dict", &dict,
		"enums?", &enums,
	)
	if err != nil {
		return nil, err
	}
	var buf strings.Builder
	for _, kv := range dict.Items() {
		key, err := dictKey(kv[0])
		if err != nil {
			return nil, err
		}
		if err := keyValueToProto(&buf, key, kv[1], ""); err != nil {
			return nil, err
		}
	}
	return starlark.String(buf.String()), nil
}

// starlark2diviner translates a Starlark value to a Diviner value.
// Nil is returned when conversion is impossible.
func starlark2diviner(val starlark.Value) diviner.Value {
	switch val := val.(type) {
	case starlark.String:
		return diviner.String(val.GoString())
	case starlark.Float:
		return diviner.Float(float64(val))
	case starlark.Int:
		v64, ok := val.Int64()
		if !ok {
			return nil
		}
		return diviner.Int(v64)
	case starlark.Bool:
		return diviner.Bool(bool(val))
	case *starlark.List:
		list := make(diviner.List, val.Len())
		for i := range list {
			list[i] = starlark2diviner(val.Index(i))
			if list[i] == nil {
				return nil
			}
		}
		return &list
	case *starlark.Dict:
		dict := make(diviner.Dict, val.Len())
		for _, kv := range val.Items() {
			subkey, err := dictKey(kv[0])
			if err != nil {
				return nil
			}
			subval := starlark2diviner(kv[1])
			if subval == nil {
				return nil
			}
			dict[subkey] = subval
		}
		return &dict
	default:
		return nil
	}
}

// diviner2starlark translates a Diviner value to a Starlark Value.
// Nil is returned when the conversion is impossible.
func diviner2starlark(val diviner.Value) starlark.Value {
	switch val.(type) {
	case diviner.Float, *diviner.Float:
		return starlark.Float(val.Float())
	case diviner.Int, *diviner.Int:
		return starlark.MakeInt64(val.Int())
	case diviner.String, *diviner.String:
		return starlark.String(val.String())
	case diviner.Bool, *diviner.Bool:
		return starlark.Bool(val.Bool())
	case *diviner.List:
		elems := make([]starlark.Value, val.Len())
		for i := range elems {
			elems[i] = diviner2starlark(val.Index(i))
		}
		return starlark.NewList(elems)
	default:
		return nil
	}
}
