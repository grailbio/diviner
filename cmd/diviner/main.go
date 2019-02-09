// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// BUG(marius): We currently use a hardcoded bigmachine
// configuration; make this configurable via the diviner config.

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
// used and training batch size:
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
//	diviner config.dv run study [-trials N] [-parallelism P]
//	diviner config.dv summarize study
//
// diviner config.dv list lists the studies provided by the
// configuration config.dv.
//
// diviner config.dv run study [-trials N] [-parallelism P] conducts
// N trials of the named study as configured in config.dv, using a
// maximum parallelism of P. If N not provided, then all trails are
// run, as long as the set of trials are finite. If P is not defined,
// then diviner runs all trials in parallel.
//
// diviner config.dv summarize study summarizes the named study; it
// outputs a TSV of all known trials, specifying parameter values.
// The TSV is ordered by the objective.
//
// [1] https://www.kdd.org/kdd2017/papers/view/google-vizier-a-service-for-black-box-optimization
// [2] https://docs.bazel.build/versions/master/skylark/language.html
package main

import (
	"context"
	"expvar"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sort"
	"text/tabwriter"

	"github.com/grailbio/base/log"
	"github.com/grailbio/base/tsv"
	"github.com/grailbio/bigmachine"
	"github.com/grailbio/bigmachine/ec2system"
	"github.com/grailbio/diviner"
	"github.com/grailbio/diviner/divinerdb"
	"github.com/grailbio/diviner/oracle"
	"github.com/grailbio/diviner/runner"
	"github.com/grailbio/diviner/script"
)

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
	diviner config.dv list
		List studies available in the provided configuration.
	diviner config.dv run study [-trials N] [-parallelism P]
		Run N trials of the given study with parallelism P.
	diviner config.dv summarize study
		Summarizes the known set of trials in the provided study.

Flags:`)
	flag.PrintDefaults()
	os.Exit(2)
}

var (
	httpaddr = flag.String("http", ":6000", "http status address")
	dir      = flag.String("dir", "", "prefix where state is stored")
)

func main() {
	// This is a temporary hack required to bootstrap worker nodes
	// without also shipping the run spec.
	//
	// TODO(marius): fix this in bigmachine itself.
	if os.Getenv("BIGMACHINE_SYSTEM") == "ec2" {
		system := new(ec2system.System)
		log.Fatal(bigmachine.Start(system))
	}
	log.SetPrefix("")
	log.AddFlags()
	flag.Usage = usage
	flag.Parse()
	if flag.NArg() < 2 {
		flag.Usage()
	}
	studies, err := script.Load(flag.Arg(0), nil)
	if err != nil {
		log.Fatalf("error loading config %s: %v", flag.Arg(0), err)
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
		run(study, flag.Args()[3:])
	case "summarize":
		if flag.NArg() < 3 {
			flag.Usage()
		}
		study := find(studies, flag.Arg(2))
		summarize(study, flag.Args()[3:])
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

func run(study diviner.Study, args []string) {
	var (
		flags       = flag.NewFlagSet("run", flag.ExitOnError)
		ntrials     = flags.Int("trials", 0, "number of trials to run")
		parallelism = flags.Int("parallelism", 0, "maximum run parallelism")
	)
	if err := flags.Parse(args); err != nil {
		log.Fatal(err)
	}
	if flags.NArg() != 0 {
		flag.Usage()
	}
	ctx := context.Background()
	// TODO(marius): don't hardcode this; specify it in the study.
	system := &ec2system.System{
		AMI:             "ami-09294617227072dcc",
		InstanceProfile: "arn:aws:iam::619867110810:instance-profile/adhoc",
		InstanceType:    "p3.2xlarge",
		Diskspace:       300,
		Dataspace:       800,
		OnDemand:        true,
		Flavor:          ec2system.Ubuntu,
	}
	b := bigmachine.Start(system)
	if study.Oracle == nil {
		study.Oracle = oracle.GridSearch
	}
	b.HandleDebug(http.DefaultServeMux)
	go func() {
		err := http.ListenAndServe(*httpaddr, nil)
		log.Error.Printf("failed to start diagnostic http server: %v", err)
	}()

	db := divinerdb.New(*dir)
	runner := runner.New(study, db, b, *parallelism)
	expvar.Publish("diviner", expvar.Func(func() interface{} { return runner.Counters() }))
	http.Handle("/status", runner)
	if _, ok := study.Oracle.(*oracle.Skopt); ok {
		nround := (*ntrials + *parallelism - 1) / *parallelism
		log.Printf("running %d rounds in iterative mode", nround)
		var total int
		for round := 0; round < nround; round++ {
			howmany := *parallelism
			if total+howmany > *ntrials {
				howmany = *ntrials - total
			}
			log.Printf("running %d trials in round %d", howmany, round)
			_, err := runner.Do(ctx, howmany)
			if err != nil {
				log.Fatal(err)
			}
		}
		return
	}
	done, err := runner.Do(ctx, *ntrials)
	if err != nil {
		log.Fatal(err)
	}
	if done {
		log.Printf("study %s complete", study.Name)
	}
}

func summarize(study diviner.Study, args []string) {
	if len(args) != 0 {
		flag.Usage()
	}
	db := divinerdb.New(*dir)
	trials, err := db.Load(study)
	if err != nil {
		log.Fatalf("error loading trials for study %s: %v", study.Name, err)
	}
	// Print some study metadata.
	var tw tabwriter.Writer
	tw.Init(os.Stdout, 4, 4, 1, ' ', 0)
	fmt.Fprintf(&tw, "# Study %q:\n", study.Name)
	for _, param := range study.Params.Sorted() {
		fmt.Fprintf(&tw, "#	param %s:	%s\n", param.Name, param)
	}
	fmt.Fprintf(&tw, "#	objective: %s\n", study.Objective)
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
