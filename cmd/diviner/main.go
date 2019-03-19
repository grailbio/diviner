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
// machine under its default user and environment. The environment
// variable DIVINER_RUN_ID contains the trial's run id (in the form of
// study_name:run_number) so that, for example, the script's output
// may be persisted by the trial's name and looked up later.
//
// With a configuration in hand, the diviner tool is used to conduct trails,
// and examine study results.
//
// Usage:
//	diviner list [-runs] studies...
//		List studies available studies or runs.
//	diviner list -l script.dv studies...
//		List studies available studies defined in script.dv.
//	diviner info [-v] names...
//		Display information for the given study or run names.
//	diviner leaderboard [-objective objective] [-n N] [-values values] [-metrics metrics] studies...
//		Display a leaderboard of all trails in the provided studies. The leaderboard
//		uses the studies' shared objective unless overridden.
//	diviner run [-rounds M] [-trials N] script.dv [studies]
//		Run M rounds of N trials of the studies matching regexp.
//		All studies are run if the regexp is omitted.
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
// diviner list -l script.dv studies... lists information for all of
// the studies defined in the provided script matching the studies
// regular expressions. If no patterns are provided, then all studies
// are shown.
//
// diviner info [-v] names... displays detailed information about the
// matching study or run names. If -v is given then even more verbose
// output is given.
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
// diviner logs [-f] run writes logs from the named run to standard
// output. If -f is given, the log is followed and updates are written
// as they appear.
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
	"regexp"
	"sort"
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
	"golang.org/x/sync/errgroup"
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
	diviner info [-v] names...
		Display information for the given study or run names.
	diviner leaderboard [-objective objective] [-n N] [-values values] [-metrics metrics] studies...
		Display a leaderboard of all trails in the provided studies. The leaderboard
		uses the studies' shared objective unless overridden.
	diviner run [-rounds M] [-trials N] script.dv [studies]
		Run M rounds of N trials of the studies matching regexp.
		All studies are run if the regexp is omitted.
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
	case "run":
		run(database, args)
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
		flags    = flag.NewFlagSet("list", flag.ExitOnError)
		listRuns = flags.Bool("runs", false, "list runs matching studies")
		load     = flags.String("l", "", "load studies from the provided script file")
		runState = flags.String("state", "pending,success,failure", "list of run states to query")
		status   = flags.Bool("s", false, "show status for pending runs")
	)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, `usage:
	diviner list [-runs] studies...
	diviner list -l script.dv studies...

List prints a summary overview of all studies (or runs) that match
the given study names.`)
		flags.PrintDefaults()
		os.Exit(2)
	}
	if err := flags.Parse(args); err != nil {
		log.Fatal(err)
	}
	if *listRuns && *load != "" {
		flags.Usage()
	}
	ctx := context.Background()
	args = flags.Args()
	if len(args) == 0 {
		args = []string{".*"} // list all
	}
	getter := databaseGetter(db)
	if *load != "" {
		getter = func(_ context.Context, prefix string) []diviner.Study {
			studies, err := script.Load(*load, nil)
			if err != nil {
				log.Fatal(err)
			}
			var n int
			for _, study := range studies {
				if strings.HasPrefix(study.Name, prefix) {
					studies[n] = study
					n++
				}
			}
			return studies[:n]
		}
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
		runs[i], err = db.Runs(ctx, studies[i].Name, state)
		return err
	})
	if err != nil {
		log.Fatal(err)
	}
	var statuses map[diviner.Run]string
	if *status {
		// TODO(marius): This way of querying for runs and statuses is a
		// bit of a messy hodgepodge. We should provide a single way of
		// querying studies and runs, and have a clear separation between
		// update methods and query methods. This will allow us to push
		// queries down where they can be efficiently executed.
		statuses = make(map[diviner.Run]string)
		var (
			mu   sync.Mutex
			g, _ = errgroup.WithContext(ctx)
		)
		for i := range runs {
			for _, run := range runs[i] {
				if run.State() == diviner.Pending {
					run := run
					g.Go(func() error {
						status, err := run.Status(ctx)
						if err != nil {
							log.Error.Printf("%s: failed to retrieve status: %v", run, err)
							return nil
						}
						mu.Lock()
						defer mu.Unlock()
						statuses[run] = status
						return nil
					})
				}
			}
		}
		if err := g.Wait(); err != nil {
			log.Fatal(err)
		}
	}
	now := time.Now()
	for i, study := range studies {
		for _, run := range runs[i] {
			var layout = time.Kitchen
			switch dur := now.Sub(run.Created()); {
			case dur > 7*24*time.Hour:
				layout = "2Jan06"
			case dur > 24*time.Hour:
				layout = "Mon3:04PM"
			}
			fmt.Fprintf(&tw, "%s:%s\t%s\t%s\t%s\t%s\n",
				study.Name, run.ID(), run.Created().Local().Format(layout),
				run.Runtime(), run.State(), statuses[run])
		}
	}
	tw.Flush()
}

var (
	studyTemplate = template.Must(template.New("study").Parse(`study {{.Name}}:
	objective:	{{.Objective}}{{range $_, $value := .Params.Sorted }}
	{{$value.Name}}:	{{$value.Param}}{{end}}
	oracle:	{{printf "%T" .Oracle}}
`))

	runFuncMap = template.FuncMap{
		"reindent": reindent,
	}

	runTemplate = template.Must(template.New("study").Funcs(runFuncMap).Parse(`run {{.study}}:{{.run.ID}}:
	state:	{{.run.State}}{{if .status}}
	status:	{{.status}}{{end}}
	created:	{{.run.Created.Local}}
	runtime:	{{.run.Runtime}}
	values:{{range $_, $value := .run.Values.Sorted }}
		{{$value.Name}}:	{{$value.Value}}{{end}}
	metrics:{{range $_, $metric := .metrics.Sorted }}
		{{$metric.Name}}:	{{$metric.Value}}{{end}}{{if .verbose}}
	script:
{{reindent "		" .run.Config.Script}}{{end}}
`))
)

func info(db diviner.Database, args []string) {
	flags := flag.NewFlagSet("list", flag.ExitOnError)
	verbose := flags.Bool("v", false, "verbose output")
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, `usage: diviner info [-v] ids...

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
	ctx := context.Background()
	var tw tabwriter.Writer
	tw.Init(os.Stdout, 4, 4, 1, ' ', 0)
	for _, arg := range flags.Args() {
		study, run := splitName(arg)
		if run == "" {
			study, err := db.Study(ctx, study)
			if err != nil {
				log.Fatal(err)
			}
			if err := studyTemplate.Execute(&tw, study); err != nil {
				log.Fatal(err)
			}
		} else {
			run, err := db.Run(ctx, study, run)
			if err != nil {
				log.Fatal(err)
			}
			metrics, err := run.Metrics(ctx)
			if err != nil {
				log.Fatal(err)
			}
			var status string
			if run.State() == diviner.Pending {
				status, err = run.Status(ctx)
				if err != nil {
					log.Error.Printf("failed retrieving status for run %s: %v", run.ID(), err)
				}
			}
			err = runTemplate.Execute(&tw, map[string]interface{}{
				"study":   study,
				"run":     run,
				"metrics": metrics,
				"verbose": *verbose,
				"status":  status,
			})
			if err != nil {
				log.Fatal(err)
			}
		}
	}
	tw.Flush()
}

func run(db diviner.Database, args []string) {
	var (
		flags   = flag.NewFlagSet("run", flag.ExitOnError)
		ntrials = flags.Int("trials", 1, "number of trials to run in each round")
		nrounds = flags.Int("rounds", 1, "number of rounds to run")
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
			study = run == ""
		} else if run == "" != study {
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
			study, run := splitName(args[i])
			runs[i], err = db.Run(ctx, study, run)
			return
		})
		if err != nil {
			log.Fatal(err)
		}
		err = traverse.Each(len(runs), func(i int) (err error) {
			log.Printf("repeating run %s", args[i])
			runs[i], err = runner.Run(ctx, runsStudy[i], runs[i].Values())
			return
		})
		if err != nil {
			log.Fatal(err)
		}
		for i, run := range runs {
			if run.State() != diviner.Success {
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

func leaderboard(db diviner.Database, args []string) {
	var (
		flags             = flag.NewFlagSet("leaderboard", flag.ExitOnError)
		objectiveOverride = flags.String("objective", "", "objective to use instead of studies' shared objective")
		numEntries        = flags.Int("n", 10, "number of top trials to display")
		valuesRe          = flags.String("values", "^$", "regular expression matching parameter values to display")
		metricsRe         = flags.String("metrics", "^$", "regular expression matching additional metrics to display")
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
	studies := studies(ctx, flags.Args(), databaseGetter(db))
	if len(studies) == 0 {
		log.Fatal("no studies matched")
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
		switch {
		case strings.HasPrefix(*objectiveOverride, "-"):
			objective.Direction = diviner.Minimize
			objective.Metric = (*objectiveOverride)[1:]
		case strings.HasPrefix(*objectiveOverride, "+"):
			objective.Direction = diviner.Maximize
			objective.Metric = (*objectiveOverride)[1:]
		default:
			log.Fatalf("invalid objective %s: must start with + or -", *objectiveOverride)
		}
	}
	studyRuns := make([][]diviner.Run, len(studies))
	err := traverser.Each(len(studyRuns), func(i int) (err error) {
		studyRuns[i], err = db.Runs(ctx, studies[i].Name, diviner.Success)
		return
	})
	if err != nil {
		log.Fatal(err)
	}

	type runMetrics struct {
		diviner.Study
		diviner.Run
		diviner.Metrics
	}
	var runs []runMetrics
	for i, list := range studyRuns {
		for _, run := range list {
			runs = append(runs, runMetrics{Study: studies[i], Run: run})
		}
	}
	err = traverser.Each(len(runs), func(i int) (err error) {
		runs[i].Metrics, err = runs[i].Run.Metrics(ctx)
		return
	})
	if err != nil {
		log.Fatal(err)
	}
	var (
		n       int
		values  = make(map[string]bool)
		metrics = make(map[string]bool)
	)
	for _, run := range runs {
		if _, ok := run.Metrics[objective.Metric]; !ok {
			continue
		}
		runs[n] = run
		n++
		for name := range run.Metrics {
			metrics[name] = true
		}
		for name := range run.Values() {
			values[name] = true
		}
	}
	if n < len(runs) {
		log.Printf("skipping %d runs due to missing metrics", len(runs)-n)
	}
	sort.SliceStable(runs, func(i, j int) bool {
		iv, jv := runs[i].Metrics[objective.Metric], runs[j].Metrics[objective.Metric]
		switch objective.Direction {
		case diviner.Maximize:
			return jv < iv
		case diviner.Minimize:
			return iv < jv
		default:
			panic(objective)
		}
	})
	if *numEntries > 0 && len(runs) > *numEntries {
		runs = runs[:*numEntries]
	}
	delete(metrics, objective.Metric)
	var (
		metricsOrdered = filterAndSort(metrics, *metricsRe)
		valuesOrdered  = filterAndSort(values, *valuesRe)
		tw             tabwriter.Writer
	)
	tw.Init(os.Stdout, 4, 4, 1, ' ', 0)
	fmt.Fprintf(&tw, "run\t%s", objective.Metric)
	if len(metricsOrdered) > 0 {
		fmt.Fprint(&tw, "\t"+strings.Join(metricsOrdered, "\t"))
	}
	if len(valuesOrdered) > 0 {
		fmt.Fprint(&tw, "\t"+strings.Join(valuesOrdered, "\t"))
	}
	fmt.Fprintln(&tw)
	for _, run := range runs {
		fmt.Fprintf(&tw, "%s:%s\t%.4g", run.Study.Name, run.Run.ID(), run.Metrics[objective.Metric])
		if len(metricsOrdered) > 0 {
			metrics := make([]string, len(metricsOrdered))
			for i, name := range metricsOrdered {
				if v, ok := run.Metrics[name]; ok {
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
				if v, ok := run.Values()[name]; ok {
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
	ctx := context.Background()
	parts := strings.SplitN(flags.Arg(0), ":", 2)
	if len(parts) != 2 {
		log.Fatalf("invalid run name %s", flags.Arg(0))
	}
	run, err := db.Run(ctx, parts[0], parts[1])
	if err != nil {
		log.Fatalf("error retrieving run %s: %v", flags.Arg(0), err)
	}
	if _, err := io.Copy(os.Stdout, run.Log(*follow)); err != nil {
		log.Fatal(err)
	}
}

func databaseGetter(db diviner.Database) func(context.Context, string) []diviner.Study {
	return func(ctx context.Context, prefix string) []diviner.Study {
		studies, err := db.Studies(ctx, prefix)
		if err != nil {
			log.Fatal(err)
		}
		return studies
	}
}

func studies(ctx context.Context, args []string, get func(context.Context, string) []diviner.Study) []diviner.Study {
	var studies []diviner.Study
	for _, arg := range args {
		re, err := regexp.Compile(`^` + arg + `$`)
		if err != nil {
			log.Fatal(err)
		}
		// TODO(marius): make point query if complete
		prefix, _ := re.LiteralPrefix()
		matched := get(ctx, prefix)
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

func filterAndSort(keys map[string]bool, match string) []string {
	re, err := regexp.Compile(match)
	if err != nil {
		log.Fatal(err)
	}
	sorted := make([]string, 0, len(keys))
	for k := range keys {
		if !re.MatchString(k) {
			delete(keys, k)
			continue
		}
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)
	return sorted
}

func splitName(name string) (study, run string) {
	parts := strings.SplitN(name, ":", 2)
	switch len(parts) {
	case 1:
		return parts[0], ""
	case 2:
		if parts[1] == "" {
			log.Fatalf("%s: empty run ID", name)
		}
		return parts[0], parts[1]
	default:
		panic(parts)
	}
}
