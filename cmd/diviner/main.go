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
// parameterize the process under optimization;
//
// - a value is a concrete value for a parameter;
//
// - a metric is a numeric value produced by the process under
// optimization;
//
// - an objective is a metric to maximize or minimize;
//
// - a trial a set of values and the metrics that result from
// the process run under these values;
//
// - an oracle is an algorithm that picks a set of trials to
// perform given a history of trials;
//
// - a black box defines the process under optimization, namely:
// how to run a trial given a set of values; and finally
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
// Scripts defined in a trial's run configuration are run on the specified
// machine under its default user and environment.
//
// With a configuration in hand, the diviner tool is used to conduct trails,
// and examine study results.
//
// Usage:
//	diviner list [-runs] studies...
//		List studies available studies or runs.
//	diviner list -l script.dv [-runs] studies...
//		List studies available studies defined in script.dv.
//	diviner info [-v] [-l script] names...
//		Display information for the given study or run names.
//	diviner metrics id
//		Writes all metrics reported by the named run in TSV format.
//	diviner leaderboard [-objective objective] [-n N] [-values values] [-metrics metrics] studies...
//		Display a leaderboard of all trails in the provided studies. The leaderboard
//		uses the studies' shared objective unless overridden.
//	diviner run [-rounds M] [-trials N] script.dv [studies]
//		Run M rounds of N trials of the studies matching regexp.
//		All studies are run if the regexp is omitted.
//	diviner script script.dv study [-param=value...]
//		Render a bash script implementing the study from script.dv
//		with the provided parameters.
//	diviner run script.dv runs...
//		Re-run previous runs in studies defined in the script.dv.
//		This uses parameter values from a previous run(s) and
// 		re-runs them.
//	diviner logs [-f] run
//		Write the logs for the given run to standard output.
//
// diviner list [-runs] studies... lists the studies matching the regular
// expressions given. If -runs is specified then the study's runs are
// listed instead.
//
// diviner list -l script.dv [-runs] studies... lists information for all of
// the studies defined in the provided script matching the studies
// regular expressions. If no patterns are provided, then all studies
// are shown. If -runs is given, the matching studies' runs are listed
// instead.
//
// diviner info [-v] [-l script] names... displays detailed
// information about the matching study or run names. If -v is given
// then even more verbose output is given. If -l is given, then
// studies are loaded from the provided script.
//
// diviner metrics id writes all metrics reported by the provided run
// to standard output in TSV format. Every unique metric name
// reported over time is a single column; missing values are denoted
// by "NA".
//
// diviner leaderboard [-objective objective] [-n N] [-values values]
// [-metrics metrics] studies... displays a leaderboard of all trials
// matching the provided studies. The leaderboard is ordered by the
// studies' shared objective unless overridden the -objective flag.
// Parameter values and additional metrics may be displayed by
// providing regular expressions to the -values and -metrics flags
// respectively.
//
// diviner run [-rounds M] [-trials N] script.dv [studies] performs
// trials as defined in the provided script. M rounds of N trials
// each are performed for each of the studies that matches the
// argument. If no studies are specified, all studies are run
// concurrently.
//
// diviner run script.dv runs... re-runs one or more runs from
// studies defined in the provided script. Specifically: parameter
// values are taken from the named runs and re-launched with the
// current version of the study from the script.
//
// diviner script script.dv study [-param=value...] renders a bash
// script containing functions for each of the study's datasets as
// well as the study itself. This is mostly intended for debugging
// and development, but may also be used to manually recreate the
// actions taken by a run.
//
// diviner logs [-f] run writes logs from the named run to standard
// output. If -f is given, the log is followed and updates are written
// as they appear.
//
// [1] https://www.kdd.org/kdd2017/papers/view/google-vizier-a-service-for-black-box-optimization
// [2] https://docs.bazel.build/versions/master/skylark/language.html
package main

import (
	"context"
	"encoding/csv"
	"expvar"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"text/tabwriter"
	"text/template"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/base/file"
	"github.com/grailbio/base/file/s3file"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/traverse"
	"github.com/grailbio/bigmachine"
	"github.com/grailbio/bigmachine/ec2system"
	"github.com/grailbio/diviner"
	"github.com/grailbio/diviner/dydb"
	"github.com/grailbio/diviner/localdb"
	"github.com/grailbio/diviner/oracle"
	"github.com/grailbio/diviner/runner"
	"github.com/grailbio/diviner/script"
)

func initS3() {
	file.RegisterImplementation("s3", func() file.Implementation {
		return s3file.NewImplementation(s3file.NewDefaultProvider(
			session.Options{}),
			s3file.Options{})
	})
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
	diviner list [-runs] studies...
		List studies available studies or runs.
	diviner list -l script.dv studies...
		List studies available studies defined in script.dv.
	diviner info [-v] [-l script] names...
		Display information for the given study or run names.
	diviner metrics id
		Writes all metrics reported by the named run in TSV format.
	diviner leaderboard [-objective objective] [-n N] [-values values] [-metrics metrics] studies...
		Display a leaderboard of all trails in the provided studies. The leaderboard
		uses the studies' shared objective unless overridden.
	diviner run [-rounds M] [-trials N] script.dv [studies]
		Run M rounds of N trials of the studies matching regexp.
		All studies are run if the regexp is omitted.
	diviner script script.dv study [-param=value...]
		Render a bash script containing functions implementing a study,
		including its datasets.
	diviner logs [-f] run
		Write the logs for the given run to standard output.

Whenever studies are named in commands, they are interpreted as
anchored regular expressions. Thus a given study name without any
regular expression control characters matches the study exactly. The
-help flag for each subcommand provides detailed documentation for
the command.

Flags:`)
	flag.PrintDefaults()
	os.Exit(2)
}

var httpaddr = flag.String("http", ":6000", "http status address")

var traverser = traverse.Limit(400)

func main() {
	initS3()
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
	log.SetFlags(log.Ldate | log.Ltime)

	databaseConfig := flag.String("db", "dynamodb,diviner-patchcnn", "database table where state is stored")
	flag.Usage = usage
	flag.Parse()
	if flag.NArg() == 0 {
		flag.Usage()
	}
	var database diviner.Database
	parts := strings.SplitN(*databaseConfig, ",", 2)
	if len(parts) != 2 {
		log.Fatalf("invalid database config %s", *databaseConfig)
	}
	switch kind, table := parts[0], parts[1]; kind {
	case "local":
		var err error
		database, err = localdb.Open(table)
		if err != nil {
			log.Fatal(err)
		}
	case "dynamodb":
		database = dydb.New(session.New(), table)
	default:
		log.Fatalf("invalid database kind %s", kind)
	}

	args := flag.Args()[1:]
	switch flag.Arg(0) {
	case "list":
		list(database, args)
	case "info":
		info(database, args)
	case "metrics":
		metrics(database, args)
	case "run":
		run(database, args)
	case "script":
		showScript(database, args)
	case "leaderboard":
		leaderboard(database, args)
	case "logs":
		logs(database, args)
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
	fmt.Fprintf(os.Stderr, "study %s not defined in config %s\n", name, flag.Arg(0))
	os.Exit(2)
	panic("not reached")
}

func match(studies *[]diviner.Study, pat string) {
	r, err := regexp.Compile(pat)
	if err != nil {
		log.Fatal(err)
	}
	var n int
	for _, study := range *studies {
		if r.MatchString(study.Name) {
			(*studies)[n] = study
			n++
		}
	}
	if n == 0 {
		log.Fatalf("no studies matched %s", r)
	}
	*studies = (*studies)[:n]
}

func list(db diviner.Database, args []string) {
	var (
		flags     = flag.NewFlagSet("list", flag.ExitOnError)
		listRuns  = flags.Bool("runs", false, "list runs matching studies")
		load      = flags.String("l", "", "load studies from the provided script file")
		runState  = flags.String("state", "pending,success,failure", "list of run states to query")
		status    = flags.Bool("s", false, "show status for pending runs")
		sinceFlag = flags.String("since", "", "only show entries that have been updated since the provided date or duration")
		valuesRe  = flags.String("values", "^$", "comma-separated list of anchored regular expression matching parameter values to display")
	)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, `usage:
	diviner list [-runs] studies...
	diviner list -l script.dv [-runs] studies...

List prints a summary overview of all studies (or runs) that match
the given study names.`)
		flags.PrintDefaults()
		os.Exit(2)
	}
	if err := flags.Parse(args); err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	args = flags.Args()
	if len(args) == 0 {
		args = []string{".*"} // list all
	}
	var since time.Time
	if *sinceFlag != "" {
		if d, err := time.ParseDuration(*sinceFlag); err == nil {
			since = time.Now().Add(-d)
		} else if since, err = time.Parse("2006-01-02", *sinceFlag); err != nil {
			fmt.Fprintf(os.Stderr, "-since=%s does not parse as a duration or date\n", *sinceFlag)
			flags.Usage()
		}
	}
	// This is a hack to make sure we don't overscan studies when looking at pending
	// runs. One hour is more than enough slack for keepalive; but this really should
	// be pushed into the database layer.
	if since.IsZero() && *runState == "pending" && *listRuns {
		since = time.Now().Add(-time.Hour)
	}
	getter := databaseGetter(db, since)
	if *load != "" {
		getter = scriptGetter(*load)
	}
	studies := studies(ctx, args, getter)
	if !*listRuns {
		for _, study := range studies {
			fmt.Println(study.Name)
		}
		return
	}
	var tw tabwriter.Writer
	tw.Init(os.Stdout, 4, 4, 1, ' ', 0)
	var state diviner.RunState
	for _, s := range strings.Split(*runState, ",") {
		switch s {
		case "pending":
			state |= diviner.Pending
		case "success":
			state |= diviner.Success
		case "failure":
			state |= diviner.Failure
		default:
			log.Fatalf("invalid run state %s", state)
		}
	}
	if state == 0 {
		log.Fatal("no run states given")
	}
	runs := make([][]diviner.Run, len(studies))
	err := traverser.Each(len(runs), func(i int) (err error) {
		runs[i], err = db.ListRuns(ctx, studies[i].Name, state, since)
		return err
	})
	if err != nil {
		log.Fatal(err)
	}
	valueKeys := make(map[string]bool)
	for i := range studies {
		for _, run := range runs[i] {
			for key := range run.Values {
				valueKeys[key] = true
			}
		}
	}
	var (
		valuesOrdered = matchAndSort(valueKeys, *valuesRe)
		values        = make([]string, len(valuesOrdered))
		now           = time.Now()
	)
	for i, study := range studies {
		sort.Slice(runs[i], func(j, k int) bool {
			return runs[i][j].Seq < runs[i][k].Seq
		})
		for _, run := range runs[i] {
			var layout = time.Kitchen
			switch dur := now.Sub(run.Created); {
			case dur > 7*24*time.Hour:
				layout = "2Jan06"
			case dur > 24*time.Hour:
				layout = "Mon3:04PM"
			}
			runtime := run.Runtime
			runtime -= runtime % time.Second
			if !*status {
				run.Status = ""
			}
			fmt.Fprintf(&tw, "%s:%d\t%s\t%s\t%s\t%s",
				study.Name, run.Seq, run.Created.Local().Format(layout),
				runtime, run.State, run.Status)
			values = values[:0]
			for _, key := range valuesOrdered {
				if v, ok := run.Values[key]; ok {
					values = append(values, fmt.Sprintf("%s:%s", key, v))
				}
			}
			if len(values) > 0 {
				fmt.Fprint(&tw, "\t", strings.Join(values, " "))
			}
			fmt.Fprintln(&tw)
		}
	}
	tw.Flush()
}

var (
	studyTemplate = template.Must(template.New("study").Parse(`study {{.Name}}:
	objective:	{{.Objective}}{{range $_, $value := .Params.Sorted }}
	{{$value.Name}}:	{{$value.Param}}{{end}}
	oracle:	{{printf "%T" .Oracle}}
	replicates:	{{.Replicates}}
`))

	runFuncMap = template.FuncMap{
		"reindent": reindent,
		"join":     strings.Join,
	}

	runTemplate = template.Must(template.New("study").Funcs(runFuncMap).Parse(`run {{.study}}:{{.run.Seq}}:
	state:	{{.run.State}}{{if .status}}
	status:	{{.run.Status}}{{end}}
	created:	{{.run.Created.Local}}
	runtime:	{{.run.Runtime}}
	restarts:	{{.run.Retries}}
	replicate:	{{.run.Replicate}}
	values:{{range $_, $value := .run.Values.Sorted }}
		{{$value.Name}}:	{{$value.Value}}{{end}}{{if .verbose}}{{range $index, $metrics := .run.Metrics}}
	metrics[{{$index}}]:{{range $_, $metric := $metrics.Sorted}}
		{{$metric.Name}}:	{{$metric.Value}}{{end}}{{end}}{{else}}
	metrics:{{range $_, $metric := .run.Trial.Metrics.Sorted }}
		{{$metric.Name}}:	{{$metric.Value}}{{end}}{{end}}{{if .verbose}}
	script:
{{reindent "		" .run.Config.Script}}{{end}}
`))

	runConfigTemplate = template.Must(template.New("run_config").Funcs(runFuncMap).Parse(`{{range $_, $dataset :=  .Datasets}}function dataset{{$dataset.Name}} {
#	if_not_exist:	{{$dataset.IfNotExist}}
#	local_files:	{{join $dataset.LocalFiles ", "}}

{{$dataset.Script}}
}{{end}}
function study {
#	local_files:	{{join .LocalFiles ", "}}
{{.Script}}
}
`))
)

func info(db diviner.Database, args []string) {
	var (
		flags   = flag.NewFlagSet("list", flag.ExitOnError)
		verbose = flags.Bool("v", false, "show all available information")
		load    = flags.String("l", "", "load studies from the provided script file")
	)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, `usage: diviner info [-v] [-l script] ids...

Info displays detailed information about studies or runs.`)
		flags.PrintDefaults()
		os.Exit(2)
	}
	if err := flags.Parse(args); err != nil {
		log.Fatal(err)
	}
	if flags.NArg() == 0 {
		flags.Usage()
	}
	getter := databaseGetter(db, time.Time{})
	if *load != "" {
		getter = scriptGetter(*load)
	}
	ctx := context.Background()
	var tw tabwriter.Writer
	tw.Init(os.Stdout, 4, 4, 1, ' ', 0)
	for _, arg := range flags.Args() {
		study, seq := splitName(arg)
		if seq == 0 {
			study := getter(ctx, study, false)[0]
			if err := studyTemplate.Execute(&tw, study); err != nil {
				log.Fatal(err)
			}
		} else {
			run, err := db.LookupRun(ctx, study, seq)
			if err != nil {
				log.Fatal(err)
			}
			err = runTemplate.Execute(&tw, map[string]interface{}{
				"study":   study,
				"run":     run,
				"verbose": *verbose,
			})
			if err != nil {
				log.Fatal(err)
			}
		}
	}
	tw.Flush()
}

func metrics(db diviner.Database, args []string) {
	var (
		flags     = flag.NewFlagSet("metrics", flag.ExitOnError)
		metricsRe = flags.String("metrics", ".*", "comma-separated list of anchored regular expression matching metrics to display")
	)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, `usage: metrics id

Writes all metrics reported by the provided run to standard output in
TSV format. Every unique metric name reported over time is a single
column; missing values are denoted by "NA".`)
		flags.PrintDefaults()
		os.Exit(2)
	}
	if err := flags.Parse(args); err != nil {
		log.Fatal(err)
	}
	if flags.NArg() != 1 {
		flags.Usage()
	}
	study, seq := splitName(flags.Arg(0))
	if seq == 0 {
		log.Fatalf("not a valid run: %s", flags.Arg(0))
	}
	ctx := context.Background()
	run, err := db.LookupRun(ctx, study, seq)
	if err != nil {
		log.Fatal(err)
	}
	keys := make(map[string]bool)
	for _, metrics := range run.Metrics {
		for key := range metrics {
			keys[key] = true
		}
	}
	sorted := matchAndSort(keys, *metricsRe)
	w := csv.NewWriter(os.Stdout)
	w.Comma = '\t'
	if err := w.Write(sorted); err != nil {
		log.Fatal(err)
	}
	record := make([]string, len(sorted))
	for _, metrics := range run.Metrics {
		for i, key := range sorted {
			v, ok := metrics[key]
			if !ok {
				record[i] = "NA"
				continue
			}
			record[i] = strconv.FormatFloat(v, 'f', -1, 64)
		}
		if err := w.Write(record); err != nil {
			log.Fatal(err)
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		log.Fatal(err)
	}
}

func run(db diviner.Database, args []string) {
	var (
		flags     = flag.NewFlagSet("run", flag.ExitOnError)
		ntrials   = flags.Int("trials", 1, "number of trials to run in each round")
		nrounds   = flags.Int("rounds", 1, "number of rounds to run")
		replicate = flags.Int("replicate", 0, "replicate to re-run")
	)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, `usage: diviner run [-rounds n] [-trials n] script.dv [studies]

Run performs trials for the studies as specified in the given diviner
script. The rounds for each matching study is run concurrently; each
round runs up to the given number of trials at the same time.

The run command runs a diagnostic http server where individual
run status may be obtained. If a shared database is used, this may
also be used to inspect run status.

If no studies are specified, all defined studies are run concurrently.`)
		flags.PrintDefaults()
		os.Exit(2)
	}
	if err := flags.Parse(args); err != nil {
		log.Fatal(err)
	}
	if flags.NArg() < 1 {
		flags.Usage()
	}
	studies, err := script.Load(flags.Arg(0), nil)
	if err != nil {
		log.Fatal(err)
	}
	args = flags.Args()[1:]
	// Make sure they are either all runs or all studies.
	study := true // "all studies"
	for i, arg := range args {
		_, run := splitName(arg)
		if i == 0 {
			study = run == 0
		} else if run == 0 != study {
			fmt.Fprintln(os.Stderr, "cannot mix studies and runs")
			flags.Usage()
		}
	}

	go func() {
		err := http.ListenAndServe(*httpaddr, nil)
		log.Error.Printf("failed to start diagnostic http server: %v", err)
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := runner.New(db)
	go func() {
		if err := runner.Loop(ctx); err != context.Canceled {
			log.Fatal(err)
		}
	}()
	expvar.Publish("diviner", expvar.Func(func() interface{} { return runner.Counters() }))
	http.Handle("/", runner)

	if study {
		switch len(args) {
		case 0: // all studies
		case 1:
			match(&studies, args[0])
		default:
			flags.Usage()
			os.Exit(2)
		}
		names := make([]string, len(studies))
		for i, study := range studies {
			names[i] = study.Name
		}
		log.Printf("performing trials for studies: %s", strings.Join(names, ", "))

		var nerr uint32
		_ = traverser.Each(len(studies), func(i int) error {
			if err := runStudy(ctx, runner, studies[i], *ntrials, *nrounds); err != nil {
				atomic.AddUint32(&nerr, 1)
				log.Error.Printf("study %v failed: %v", studies[i], err)
			}
			return nil
		})
		if nerr > 0 {
			os.Exit(1)
		}
	} else {
		var (
			runs      = make([]diviner.Run, len(args))
			runsStudy = make([]diviner.Study, len(args))
		)
		for i := range runsStudy {
			study, _ := splitName(args[i])
			runsStudy[i] = find(studies, study)
		}
		err := traverser.Each(len(runs), func(i int) (err error) {
			study, seq := splitName(args[i])
			runs[i], err = db.LookupRun(ctx, study, seq)
			return
		})
		if err != nil {
			log.Fatal(err)
		}
		err = traverse.Each(len(runs), func(i int) (err error) {
			log.Printf("repeating run %s", args[i])
			runs[i], err = runner.Run(ctx, runsStudy[i], runs[i].Values, *replicate)
			return
		})
		if err != nil {
			log.Fatal(err)
		}
		for i, run := range runs {
			if run.State != diviner.Success {
				log.Error.Printf("run %s failed", args[i])
			}
		}
	}
}

func runStudy(ctx context.Context, runner *runner.Runner, study diviner.Study, ntrials, nrounds int) error {
	if study.Oracle == nil {
		study.Oracle = &oracle.GridSearch{}
	}
	var (
		round int
		done  bool
	)
	for ; !done && round < nrounds; round++ {
		log.Printf("%s: starting %d trials in round %d/%d", study.Name, ntrials, round, nrounds)
		var err error
		done, err = runner.Round(ctx, study, ntrials)
		if err != nil {
			return err
		}
	}
	if done {
		log.Printf("%s: study complete after %d rounds", study.Name, round)
	}
	return nil
}

func showScript(db diviner.Database, args []string) {
	flags := flag.NewFlagSet("show", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, `usage: diviner script script.dv study [-param1=value1 -param2=value2 ...]

Script renders a bash script to standard output containing a function
for each of the study's datasets and the study itself. The study's
parameter values can be specified via flags; unspecified parameter
values are sampled randomly from valid parameter values. The
study is always invoked with replicate 0.
`)
		flags.PrintDefaults()
		os.Exit(2)
	}
	ident := flags.String("ident", "run_ident", "run ID to use for the rendered run")
	if err := flags.Parse(args); err != nil {
		log.Fatal(err)
	}
	if flags.NArg() < 1 {
		flags.Usage()
	}
	getter := scriptGetter(flags.Arg(0))
	studyUsage := func() {
		fmt.Fprintln(os.Stderr, "valid studies are:")
		for _, study := range studies(context.Background(), []string{".*"}, getter) {
			fmt.Fprintf(os.Stderr, "\t%s\n", study.Name)
		}
		os.Exit(2)
	}
	if flags.NArg() < 2 {
		fmt.Fprintln(os.Stderr, "no study specified")
		studyUsage()
	}
	args = flags.Args()[2:]
	ctx := context.Background()
	matched := studies(ctx, []string{flags.Arg(1)}, getter)
	if len(matched) != 1 {
		if len(matched) == 0 {
			fmt.Fprintln(os.Stderr, "no studies matched")
		} else {
			fmt.Fprintln(os.Stderr, "multiple studies matched")
		}
		studyUsage()
	}
	var (
		study  = matched[0]
		values diviner.Values
		rng    = rand.New(rand.NewSource(0))
	)
	flags = flag.NewFlagSet(study.Name, flag.ExitOnError)
	// Sort these so that they get the same (random) value each time.
	names := make([]string, 0, len(study.Params))
	for name := range study.Params {
		names = append(names, name)
	}
	for _, name := range names {
		switch param := study.Params[name]; param.Kind() {
		default:
			log.Fatalf("unsupporter parameter %s", param)
		case diviner.Integer:
			p := new(diviner.Int)
			flags.Int64Var((*int64)(p), name, param.Sample(rng).Int(), "integer parameter")
			values[name] = p
		case diviner.Real:
			p := new(diviner.Float)
			flags.Float64Var((*float64)(p), name, param.Sample(rng).Float(), "real parameter")
			values[name] = p
		case diviner.Str:
			p := new(diviner.String)
			flags.StringVar((*string)(p), name, param.Sample(rng).Str(), "string parameter")
			values[name] = p
		case diviner.Seq:
			log.Printf("parameter %s (%s) cannot be overriden", name, param)
			values[name] = param.Sample(rng)
		}
	}
	if err := flags.Parse(args); err != nil {
		log.Fatal(err)
	}
	for name, val := range values {
		if p := study.Params[name]; !p.IsValid(val) {
			log.Fatalf("value %s is not valid for parameter %s %s", val, name, p)
		}
	}
	config, err := study.Run(values, 0, *ident)
	if err != nil {
		log.Fatal(err)
	}
	var tw tabwriter.Writer
	tw.Init(os.Stdout, 4, 4, 1, ' ', 0)
	if err := runConfigTemplate.Execute(&tw, config); err != nil {
		log.Fatal(err)
	}
	tw.Flush()
}

func leaderboard(db diviner.Database, args []string) {
	var (
		flags             = flag.NewFlagSet("leaderboard", flag.ExitOnError)
		objectiveOverride = flags.String("objective", "", "objective to use instead of studies' shared objective")
		numEntries        = flags.Int("n", 10, "number of top trials to display")
		valuesRe          = flags.String("values", ".", "comma-separated list of anchored regular expression matching parameter values to display")
		metricsRe         = flags.String("metrics", "^$", `comma-separated list of anchored regular expression matching additional metrics to display.
Each regex can be prefixed with '+' or '-'. A regex with '+' (or '-'), when combined with -best, will pick the largest (or smallest) metric from each run.`)
	)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, `usage: diviner leaderboard [-objective objective] [-n N] [-values values] [-metrics metrics] studies...

Leaderboard displays the top N performing trials from the matched
studies, as defined by the objective shared by the studies. This
objective may be overridden via the -objective flag (which accepts a
metric name, prefixed by "+" or "-" to indicate a maximizing or
minimizing objective respectively). By default the runs and their
score obtained by the objective metric is displayed; additional
metrics as well as run parameter values may be displayed by
specifying regular expressions for matching them via the flags
-metrics and -values.`)
		flags.PrintDefaults()
		os.Exit(2)
	}
	if err := flags.Parse(args); err != nil {
		log.Fatal(err)
	}
	if flags.NArg() == 0 {
		flags.Usage()
	}
	ctx := context.Background()
	studies := studies(ctx, flags.Args(), databaseGetter(db, time.Time{}))
	if len(studies) == 0 {
		log.Fatal("no studies matched")
	}

	parseObjective := func(spec string) (objective diviner.Objective) {
		switch {
		case strings.HasPrefix(spec, "-"):
			objective.Direction = diviner.Minimize
			objective.Metric = spec[1:]
		case strings.HasPrefix(spec, "+"):
			objective.Direction = diviner.Maximize
			objective.Metric = spec[1:]
		default:
			objective.Direction = diviner.Maximize
			objective.Metric = spec
		}
		return
	}

	objective := studies[0].Objective
	if *objectiveOverride == "" {
		for i := range studies {
			if i > 0 && studies[i].Objective != studies[i-1].Objective {
				log.Fatalf("studies %s and %s do not share objectives: override with -objective",
					studies[i].Name, studies[i-1].Name)
			}
		}
	} else {
		objective = parseObjective(*objectiveOverride)
	}
	type trial struct {
		diviner.Trial
		Study string
	}
	var (
		trialsMu sync.Mutex
		trials   []trial
	)
	err := traverser.Each(len(studies), func(i int) error {
		t, err := diviner.Trials(ctx, db, studies[i])
		if err != nil {
			return err
		}
		trialsMu.Lock()
		t.Range(func(_ diviner.Value, v interface{}) {
			trials = append(trials, trial{v.(diviner.Trial), studies[i].Name})
		})
		trialsMu.Unlock()
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	var (
		n       int
		values  = make(map[string]bool)
		metrics = make(map[string]bool)
	)

	for _, trial := range trials {
		if _, ok := trial.Metrics[objective.Metric]; !ok {
			continue
		}
		trials[n] = trial
		n++
		for name := range trial.Metrics {
			metrics[name] = true

		}
		for name := range trial.Values {
			values[name] = true
		}
	}
	if n < len(trials) {
		log.Printf("skipping %d trials due to missing metrics", len(trials)-n)
		trials = trials[:n]
	}
	sort.SliceStable(trials, func(i, j int) bool {
		iv, _ := trials[i].Metrics[objective.Metric]
		jv, _ := trials[j].Metrics[objective.Metric]
		switch objective.Direction {
		case diviner.Maximize:
			return jv < iv
		case diviner.Minimize:
			return iv < jv
		default:
			panic(objective)
		}
	})
	if *numEntries > 0 && len(trials) > *numEntries {
		trials = trials[:*numEntries]
	}
	delete(metrics, objective.Metric)

	var metricsOrdered []diviner.Objective
	for _, m := range strings.Split(*metricsRe, ",") {
		obj := parseObjective(m)
		re, err := regexp.Compile(obj.Metric)
		if err != nil {
			log.Fatal(err)
		}
		i := len(metricsOrdered)
		for k := range metrics {
			if !re.MatchString(k) {
				continue
			}
			metricsOrdered = append(metricsOrdered, diviner.Objective{obj.Direction, k})
			delete(metrics, k)
		}
		// We sort the expansions, but retain the order of the list of matches.
		sort.Slice(metricsOrdered[i:len(metricsOrdered)], func(i0, i1 int) bool {
			return metricsOrdered[i0-i].Metric < metricsOrdered[i1-i].Metric
		})
	}
	var (
		valuesOrdered = matchAndSort(values, *valuesRe)
		tw            tabwriter.Writer
	)
	tw.Init(os.Stdout, 4, 4, 1, ' ', 0)
	fmt.Fprintf(&tw, "study\t%s", objective.Metric)
	if len(metricsOrdered) > 0 {
		for _, metric := range metricsOrdered {
			fmt.Fprint(&tw, "\t"+metric.Metric)
		}
	}
	if len(valuesOrdered) > 0 {
		fmt.Fprint(&tw, "\t"+strings.Join(valuesOrdered, "\t"))
	}
	fmt.Fprintln(&tw)
	for _, trial := range trials {
		v := trial.Metrics[objective.Metric]
		sort.Slice(trial.Runs, func(i, j int) bool { return trial.Runs[i].Seq < trial.Runs[j].Seq })
		seqs := make([]string, len(trial.Runs))
		for i := range seqs {
			seqs[i] = fmt.Sprint(trial.Runs[i].Seq)
		}
		fmt.Fprintf(&tw, "%s:%s\t%.4g", trial.Study, strings.Join(seqs, ","), v)
		if len(metricsOrdered) > 0 {
			metrics := make([]string, len(metricsOrdered))
			for i, metric := range metricsOrdered {
				if v, ok := trial.Metrics[metric.Metric]; ok {
					metrics[i] = fmt.Sprintf("%.3g", v)
				} else {
					metrics[i] = "NA"
				}
			}
			fmt.Fprint(&tw, "\t"+strings.Join(metrics, "\t"))
		}
		if len(valuesOrdered) > 0 {
			values := make([]string, len(valuesOrdered))
			for i, name := range valuesOrdered {
				if v, ok := trial.Values[name]; ok {
					switch v.Kind() {
					default:
						values[i] = fmt.Sprint(v)
					case diviner.Real:
						values[i] = fmt.Sprintf("%.3g", v.Float())
					case diviner.Str:
						values[i] = fmt.Sprintf("%q", v.Str())
					}
				} else {
					values[i] = "NA"
				}
			}
			fmt.Fprint(&tw, "\t"+strings.Join(values, "\t"))
		}
		fmt.Fprintln(&tw)
	}
	tw.Flush()
}

func logs(db diviner.Database, args []string) {
	var (
		flags  = flag.NewFlagSet("logs", flag.ExitOnError)
		follow = flags.Bool("f", false, "follow the log: print updates as they are appended")
	)
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, `usage: diviner logs [-f] run

Logs writes a run's logs to standard output. If the -f flag is given,
the logs is followed and updates are printed as they become available.
`)
		flags.PrintDefaults()
		os.Exit(2)
	}
	if err := flags.Parse(args); err != nil {
		log.Fatal(err)
	}
	if flags.NArg() != 1 {
		flags.Usage()
	}
	study, seq := splitName(flags.Arg(0))
	if seq == 0 {
		log.Fatalf("invalid run name %s", flags.Arg(0))
	}
	if _, err := io.Copy(os.Stdout, db.Log(study, seq, *follow)); err != nil {
		log.Fatal(err)
	}
}

func databaseGetter(db diviner.Database, since time.Time) func(context.Context, string, bool) []diviner.Study {
	return func(ctx context.Context, query string, isPrefix bool) []diviner.Study {
		if !isPrefix {
			study, err := db.LookupStudy(ctx, query)
			if err != nil {
				log.Fatal(err)
			}
			return []diviner.Study{study}
		}
		studies, err := db.ListStudies(ctx, query, since)
		if err != nil {
			log.Fatal(err)
		}
		sort.Slice(studies, func(i, j int) bool {
			return studies[i].Name < studies[j].Name
		})
		return studies
	}
}

func scriptGetter(filename string) func(context.Context, string, bool) []diviner.Study {
	return func(_ context.Context, query string, isPrefix bool) []diviner.Study {
		studies, err := script.Load(filename, nil)
		if err != nil {
			log.Fatal(err)
		}
		if !isPrefix {
			return []diviner.Study{find(studies, query)}
		}
		var n int
		for _, study := range studies {
			if strings.HasPrefix(study.Name, query) {
				studies[n] = study
				n++
			}
		}
		return studies[:n]
	}
}

func studies(ctx context.Context, args []string, get func(context.Context, string, bool) []diviner.Study) []diviner.Study {
	var studies []diviner.Study
	for _, arg := range args {
		re, err := regexp.Compile(`^` + arg + `$`)
		if err != nil {
			log.Fatal(err)
		}
		// TODO(marius): make point query if complete
		prefix, complete := re.LiteralPrefix()
		matched := get(ctx, prefix, !complete)
		for _, study := range matched {
			if re.MatchString(study.Name) {
				studies = append(studies, study)
			}
		}
	}
	if len(studies) == 0 {
		return nil
	}
	sort.SliceStable(studies, func(i, j int) bool { return studies[i].Name < studies[j].Name })
	var n int
	for i := range studies {
		if studies[n].Name != studies[i].Name {
			n++
		}
		studies[n] = studies[i]
	}
	return studies[:n+1]
}

func matchAndSort(keys map[string]bool, match string) []string {
	sorted := make([]string, 0, len(keys))
	for _, m := range strings.Split(match, ",") {
		re, err := regexp.Compile(m)
		if err != nil {
			log.Fatal(err)
		}
		i := len(sorted)
		for k := range keys {
			if !re.MatchString(k) {
				continue
			}
			sorted = append(sorted, k)
			delete(keys, k)
		}
		// We sort the expansions, but retain the order of the list of matches.
		sort.Strings(sorted[i:len(sorted)])
	}
	return sorted
}

func splitName(name string) (study string, seq uint64) {
	parts := strings.SplitN(name, ":", 2)
	switch len(parts) {
	case 1:
		return parts[0], 0
	case 2:
		if parts[1] == "" {
			log.Fatalf("%s: empty run seq", name)
		}
		seq, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			log.Fatalf("invalid name %s: %v", name, err)
		}
		return parts[0], seq
	default:
		panic(parts)
	}
}
