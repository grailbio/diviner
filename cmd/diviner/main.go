// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Diviner is a black-box optimization framework that uses Bigmachine
// to distribute a large number of computationally expensive trials
// across clusters of machines.
//
// Diviner inherits some of Vizier's [1] core concepts, and adds its
// own:
//
// - a parameter describes the type and range of values that can
//   parameterize the process under optimization;
//
// - a value is a concrete value for a parameter;
//
// - a metric is a numeric value produced by the process under
//   optimization;
//
// - an objective is a metric to maximize or minimize;
//
// - a trial a set of values and the metrics that result from
//   the process run under these values;
//
// - an oracle is an algorithm that picks a set of trials to
//   perform given a history of trials;
//
// - a black box defines the process under optimization, namely:
//   how to run a trial given a set of values; and finally
//
// - a study is a black box, an objective, and an oracle.
//
// Additionally, a run config may include a dataset, which is
// a pre-processing step that's required to run the trial itself.
// Multiple run configs may depend on the same dataset; they are
// computed at most once for each session.
//
// Studies (and their associated objects) are configured by a
// configuration script written in the Starlark configuration
// language [2]. Details of the Starlark builtins provided to create
// Diviner configurations are described in package
// github.com/grailbio/diviner/script.
//
// As an example, here's a Diviner configuration that tries to
// maximize the accuracy of a model, parameterized by the optimizer
// used and training batch size. It uses an Amazon GPU instance to
// train the model but creates the dataset on the local machine.
// Run state is stored in the "testdiviner" DynamoDB table.
//
//	config(database="dynamodb", table="testdiviner")
//
//	# local defines a system using the local machine with a maximum
//	# parallelism of 2.
//	local = localsystem("local", 2)
//
//	# gpu defines a system using an AWS EC2 GPU instance; a maximum of
//	# 100 such instances will be created at any given time.
//	gpu = ec2system("gpu",
//	    parallelism=100,
//	    ami="ami-09294617227072dcc",
//	    instance_profile="arn:aws:iam::619867110810:instance-profile/adhoc",
//	    instance_type="p3.2xlarge",
//	    disk_space=300,
//	    data_space=800,
//	    on_demand=False,
//	    flavor="ubuntu",
//	)
//
//	# model_files is the set of (local) files required to run
//	# training and evaluation for the model.
//	model_files = ["neural_network.py", "utils.py"]
//
//	# create_dataset returns a dataset that's required to
//	# run the model.
//	def create_dataset(region):
//	    url = "s3://grail-datasets/regions%s" % region
//	    return dataset(
//	        name="region%s" % region,
//	        system=local,
//	        if_not_exist=url,
//	        local_files=["make_dataset.py"],
//	        script="makedataset.py %s %s" % (region, url),
//	    )
//
//	# run_model returns a run configuration for a set of
//	# parameter values.
//	def run_model(pvalues):
//	  return run_config(
//	    datasets = [create_dataset(pvalues["region"])],
//	    system=gpu,
//	    local_files=model_files,
//	    script="""
//	      python neural_network.py \
//	        --batch_size=%(batch_size)d \
//	        --optimizer=%(optimizer)s
//	    """ % pvalues,
//	  )
//
//	# Define a black box that defines a set of parameters for
//	# neural_network.py, and provides run configs as above.
//	# Black boxes return metrics to Diviner by writing them
//	# to standard output: every line with the prefix
//	# "METRICS: " is considered to contain metrics for
//	# the trial. Metrics should be provided as comma-separated
//	# key-value pairs, e.g., the line:
//	#	METRICS: acc=0.4,loss=0.9
//	# indicates that the "acc" metrics has a value of 0.4 and the
//	# "loss" metric a value of 0.9.
//	neural_network = black_box(
//	  name="neural_network",
//	  params={
//	    "batch_size": discrete(16, 32, 64, 128),
//	    "optimizer": discrete("adam", "sgd"),
//	    "region": discrete("chr1", "chr2"),
//	  },
//	  run=run_model,
//	)
//
//	# Study declares a study that attempts to maximize the "acc"
//	# metric returned by the model as defined above.
//	study("hparam_study",
//	  black_box=neural_network,
//	  objective=maximize("acc"),
//	)
//
// With a configuration in hand, the diviner tool is used to conduct trails,
// and examine study results.
//
// Usage:
//	diviner config.dv list
//	diviner config.dv run study [-rounds M] [-trials N]
//	diviner config.dv summarize study
//	diviner config.dv ps
//	diviner config.dv ps studies...
//	diviner config.dv logs runID
//
// diviner config.dv list lists the studies provided by the
// configuration config.dv.
//
// diviner config.dv run study  [-rounds M] [-trials N] conducts M
// rounds (default 1) of N trials of the named study as configured in
// config.dv. If N not provided, then all trails are run, as long as
// the set of trials are finite.
//
// diviner config.dv summarize study summarizes the named study; it
// outputs a TSV of all known completed trials, specifying parameter
// values. The TSV is ordered by the objective.
//
// diviner config.dv ps returns a summary of currently ongoing runs
// for all studies. The output includes information about each
// pending run, the last line of log output (which may be a progress
// bar), and the latest reported metrics.
//
// diviner config.dv ps studies... returns a summary of currently
// ongoing runs for the provided studies. The output includes
// information about each pending run, the last line of log output
// (which may be a progress bar), and the latest reported metrics.
//
// diviner config.dv logs runID writes the log output of a run to standard output.
// The run identified by a run identifier which may be retrieved by the "summarize"
// or "list" commands.
//
// [1] https://www.kdd.org/kdd2017/papers/view/google-vizier-a-service-for-black-box-optimization
// [2] https://docs.bazel.build/versions/master/skylark/language.html
package main

import (
	"context"
	"expvar"
	"flag"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/tsv"
	"github.com/grailbio/bigmachine"
	"github.com/grailbio/bigmachine/ec2system"
	"github.com/grailbio/diviner"
	"github.com/grailbio/diviner/dydb"
	"github.com/grailbio/diviner/localdb"
	"github.com/grailbio/diviner/oracle"
	"github.com/grailbio/diviner/runner"
	"github.com/grailbio/diviner/script"
)

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
	diviner config.dv list
		List studies available in the provided configuration.
	diviner config.dv run study [-rounds M] [-trials N]
		Run M rounds of N trials of the given study.
	diviner config.dv summarize study
		Summarizes the known set of trials in the provided study.
	diviner config.dv ps
		Summarize currently ongoing trials in all trials.
	diviner config.dv ps studies...
		Summarize currently ongoing trials for the provided studies.
	diviner config.dv logs run
		Write the logs for the given run to standard output.

Flags:`)
	flag.PrintDefaults()
	os.Exit(2)
}

var httpaddr = flag.String("http", ":6000", "http status address")

func main() {
	// This is a temporary hack required to bootstrap worker nodes
	// without also shipping the run spec.
	//
	// TODO(marius): fix this in bigmachine itself.
	switch os.Getenv("BIGMACHINE_SYSTEM") {
	case "ec2":
		log.Fatal(bigmachine.Start(new(ec2system.System)))
	case "local":
		log.Fatal(bigmachine.Start(bigmachine.Local))
	}
	log.SetPrefix("")
	log.AddFlags()
	flag.Usage = usage
	flag.Parse()
	if flag.NArg() < 2 {
		flag.Usage()
	}
	studies, config, err := script.Load(flag.Arg(0), nil)
	if err != nil {
		log.Fatalf("error loading config %s: %v", flag.Arg(0), err)
	}
	var database diviner.Database
	switch config.Database {
	case script.Local:
		var err error
		database, err = localdb.Open(config.Table)
		if err != nil {
			log.Fatal(err)
		}
	case script.DynamoDB:
		database = dydb.New(session.New(), config.Table)
	default:
		log.Fatalf("invalid database type %d", config.Database)
	}

	switch flag.Arg(1) {
	case "list":
		for _, study := range studies {
			fmt.Println(study.Name)
		}
	case "run":
		if flag.NArg() < 3 {
			flag.Usage()
		}
		study := find(studies, flag.Arg(2))
		run(database, study, flag.Args()[3:])
	case "summarize":
		if flag.NArg() < 3 {
			flag.Usage()
		}
		study := find(studies, flag.Arg(2))
		summarize(database, study, flag.Args()[3:])
	case "ps":
		if flag.NArg() < 2 {
			flag.Usage()
		}
		ps(database, studies, flag.Args()[2:])
	case "logs":
		if flag.NArg() < 2 {
			flag.Usage()
		}
		logs(database, flag.Args()[2:])
	default:
		flag.Usage()
	}
}

func find(studies []diviner.Study, name string) diviner.Study {
	for _, study := range studies {
		if study.Name == name {
			return study
		}
	}
	fmt.Fprintf(os.Stderr, "study %s not defined in config %s", name, flag.Arg(0))
	os.Exit(2)
	panic("not reached")
}

func run(db diviner.Database, study diviner.Study, args []string) {
	var (
		flags   = flag.NewFlagSet("run", flag.ExitOnError)
		ntrials = flags.Int("trials", 0, "number of trials to run in each round")
		nrounds = flags.Int("rounds", 1, "number of rounds to run")
	)
	if err := flags.Parse(args); err != nil {
		log.Fatal(err)
	}
	if flags.NArg() != 0 {
		flag.Usage()
	}
	ctx := context.Background()
	if study.Oracle == nil {
		study.Oracle = &oracle.GridSearch{}
	}
	go func() {
		err := http.ListenAndServe(*httpaddr, nil)
		log.Error.Printf("failed to start diagnostic http server: %v", err)
	}()

	runner := runner.New(study, db)
	expvar.Publish("diviner", expvar.Func(func() interface{} { return runner.Counters() }))
	http.Handle("/status", runner)

	var (
		round int
		done  bool
	)
	for ; !done && round < *nrounds; round++ {
		log.Printf("starting %d trials in round %d/%d", *ntrials, round, *nrounds)
		var err error
		done, err = runner.Do(ctx, *ntrials)
		if err != nil {
			log.Fatal(err)
		}
	}
	if done {
		log.Printf("study %s complete after %d rounds", study.Name, round)
	}
}

func summarize(db diviner.Database, study diviner.Study, args []string) {
	if len(args) != 0 {
		flag.Usage()
	}
	ctx := context.Background()
	runs, err := db.Runs(ctx, study, diviner.Success)
	if err != nil {
		log.Fatalf("error loading runs for study %s: %v", study.Name, err)
	}
	type trial struct {
		diviner.Trial
		Run diviner.Run
	}
	trials := make([]trial, len(runs))
	for i, run := range runs {
		trials[i].Run = run
		trials[i].Trial.Values = run.Values()
		trials[i].Trial.Metrics, err = run.Metrics(ctx)
		if err != nil {
			log.Fatalf("error retrieving trial from run %s: %v", run, err)
		}
	}
	// Print some study metadata.
	var tw tabwriter.Writer
	tw.Init(os.Stdout, 4, 4, 1, ' ', 0)
	fmt.Fprintf(&tw, "# Study %q:\n", study.Name)
	for _, param := range study.Params.Sorted() {
		fmt.Fprintf(&tw, "#	param %s:	%s\n", param.Name, param)
	}
	fmt.Fprintf(&tw, "#	objective:	%s\n", study.Objective)
	if err := tw.Flush(); err != nil {
		log.Fatal(err)
	}
	// Filter out trials that do not have the objective metric.
	// TODO(marius): check for this when performing the trials.
	var j int
	for _, trial := range trials {
		if _, ok := trial.Metrics[study.Objective.Metric]; ok {
			trials[j] = trial
			j++
		}
	}
	if j < len(trials) {
		log.Printf("dropped %d trials due to missing metrics", len(trials)-j)
	}
	trials = trials[:j]
	if len(trials) == 0 {
		log.Print("no trials")
		os.Exit(0)
	}
	sort.Slice(trials, func(i, j int) bool {
		var (
			mi = trials[i].Metrics[study.Objective.Metric]
			mj = trials[j].Metrics[study.Objective.Metric]
		)
		switch study.Objective.Direction {
		case diviner.Maximize:
			return mj < mi
		case diviner.Minimize:
			return mi < mj
		default:
			panic(study.Objective.Direction)
		}
	})
	params := make([]string, 0, len(study.Params))
	for key := range study.Params {
		params = append(params, key)
	}
	sort.Strings(params)
	allMetrics := make(map[string]bool)
	for _, trial := range trials {
		for key := range trial.Metrics {
			if key != study.Objective.Metric {
				allMetrics[key] = true
			}
		}
	}
	metrics := make([]string, 0, len(allMetrics))
	for key := range allMetrics {
		metrics = append(metrics, key)
	}
	sort.Strings(metrics)

	w := tsv.NewWriter(os.Stdout)
	w.WriteString("run")
	for _, key := range params {
		w.WriteString(key)
	}
	w.WriteString(study.Objective.Metric)
	for _, key := range metrics {
		w.WriteString(key)
	}
	if err := w.EndLine(); err != nil {
		log.Fatal(err)
	}
	for _, trial := range trials {
		w.WriteString(trial.Run.ID())
		for _, key := range params {
			w.WriteString(trial.Values[key].String())
		}
		w.WriteFloat64(trial.Metrics[study.Objective.Metric], 'f', -1)
		for _, key := range metrics {
			if val, ok := trial.Metrics[key]; ok {
				w.WriteFloat64(val, 'f', -1)
			} else {
				w.WriteString("NA")
			}
		}
		if err := w.EndLine(); err != nil {
			log.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		log.Fatal(err)
	}
}

func ps(db diviner.Database, studies []diviner.Study, args []string) {
	if len(args) > 0 {
		keep := make(map[string]bool)
		for _, study := range args {
			keep[study] = true
		}
		var n int
		for _, study := range studies {
			if keep[study.Name] {
				studies[n] = study
				n++
			}
		}
		studies = studies[:n]
	}
	ctx := context.Background()
	if len(studies) == 0 {
		log.Printf("no studies")
		return
	}
	for _, study := range studies {
		runs, err := db.Runs(ctx, study, diviner.Pending)
		if err != nil {
			log.Fatal(err)
		}
		if len(runs) == 0 {
			continue
		}
		fmt.Printf("study %s: %s\n", study.Name, study.Params)
		sorted := make([]string, 0, len(study.Params))
		for key := range study.Params {
			sorted = append(sorted, key)
		}
		sort.Strings(sorted)
		var tw tabwriter.Writer
		tw.Init(os.Stdout, 4, 4, 1, ' ', 0)
		io.WriteString(&tw, "id\t")
		io.WriteString(&tw, strings.Join(sorted, "\t"))
		fmt.Fprintln(&tw, "\tmetrics\tstatus")
		for _, run := range runs {
			row := make([]string, len(sorted)+3)
			row[0] = fmt.Sprint(run.ID())
			status, err := run.Status(ctx)
			if err != nil {
				row[len(row)-1] = err.Error()
			} else {
				row[len(row)-1] = status
			}
			values := run.Values()
			for i, key := range sorted {
				if v, ok := values[key]; ok {
					row[i+1] = v.String()
				} else {
					row[i+1] = "NA"
				}
			}
			if metrics, err := run.Metrics(ctx); err != nil {
				row[len(row)-2] = err.Error()
			} else if len(metrics) == 0 {
				row[len(row)-2] = "NA"
			} else {
				keys := make([]string, 0, len(metrics))
				for key := range metrics {
					keys = append(keys, key)
				}
				sort.Strings(keys)
				elems := make([]string, len(keys))
				for i, key := range keys {
					elems[i] = fmt.Sprintf("%s=%f", key, metrics[key])
				}
				row[len(row)-2] = strings.Join(elems, ",")
			}
			io.WriteString(&tw, strings.Join(row, "\t"))
			fmt.Fprintln(&tw)
		}
		tw.Flush()
	}
}

func logs(db diviner.Database, args []string) {
	if len(args) != 1 {
		flag.Usage()
	}
	ctx := context.Background()
	run, err := db.Run(ctx, args[0])
	if err != nil {
		log.Fatalf("error retrieving run %s: %v", args[0], err)
	}
	if _, err := io.Copy(os.Stdout, run.Log()); err != nil {
		log.Fatal(err)
	}
}
