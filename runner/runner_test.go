// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package runner_test

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/grailbio/base/retry"
	"github.com/grailbio/bigmachine"
	"github.com/grailbio/bigmachine/testsystem"
	"github.com/grailbio/diviner"
	"github.com/grailbio/diviner/divinerdb"
	"github.com/grailbio/diviner/oracle"
	"github.com/grailbio/diviner/runner"
	"github.com/grailbio/testutil"
)

func TestRunner(t *testing.T) {
	dir, cleanup := runnerTest(t)
	defer cleanup()

	datasetFile := filepath.Join(dir, "dataset")
	dataset := diviner.Dataset{
		Name: "testset",
		Script: fmt.Sprintf(`
			# Should run only once.
			test -f %s && exit 1
			echo ran > %s
		`, datasetFile, datasetFile),
	}

	db := divinerdb.New(dir)
	study := diviner.Study{
		Name: "test",
		Params: diviner.Params{
			"param": diviner.NewDiscrete(diviner.Int(0), diviner.Int(1), diviner.Int(2)),
		},
		Run: func(values diviner.Values) diviner.RunConfig {
			return diviner.RunConfig{
				Datasets: []diviner.Dataset{dataset},
				Script: fmt.Sprintf(`
						# Dataset should have been produced.
						test -f %s || exit 1
						echo hello world
						echo METRICS: paramvalue=%s
					`, datasetFile, values["param"]),
			}
		},
		Objective: diviner.Objective{diviner.Maximize, "acc"},
		Oracle:    oracle.GridSearch,
	}
	test := testsystem.New()
	b := bigmachine.Start(test)

	r := runner.New(study, db, b, 2)
	ctx := context.Background()
	done, err := r.Do(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !done {
		t.Fatal("not done")
	}
	trials, err := db.Load(study)
	if err != nil {
		t.Fatal(err)
	}
	sort.Slice(trials, func(i, j int) bool {
		return trials[i].Values["param"].Int() < trials[j].Values["param"].Int()
	})
	expect := make([]diviner.Trial, 3)
	for i := range expect {
		expect[i] = diviner.Trial{
			Values:  diviner.Values{"param": diviner.Int(i)},
			Metrics: diviner.Metrics{"paramvalue": float64(i)},
		}
	}
	if got, want := trials, expect; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := test.N(), 2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	// Make sure the machines are stopped.
	for _, m := range b.Machines() {
		if !eventually(func() bool { return m.Err() != nil }) {
			t.Errorf("machine %v did not stop", m)
		}
	}
}

func TestRunnerError(t *testing.T) {
	dir, cleanup := runnerTest(t)
	defer cleanup()

	dataset := diviner.Dataset{
		Name:   "testset",
		Script: "exit 1",
	}
	db := divinerdb.New(dir)
	study := diviner.Study{
		Name: "test",
		Params: diviner.Params{
			"param": diviner.NewDiscrete(diviner.Int(0), diviner.Int(1)),
		},
		Run: func(values diviner.Values) diviner.RunConfig {
			config := diviner.RunConfig{Script: "exit 1"}
			if values["param"].Int() == 0 {
				// In this case, the run should fail before attempting
				// execution.
				config.Datasets = []diviner.Dataset{dataset}
			}
			return config
		},
		Objective: diviner.Objective{diviner.Maximize, "acc"},
		Oracle:    oracle.GridSearch,
	}
	test := testsystem.New()
	b := bigmachine.Start(test)

	r := runner.New(study, db, b, 2)
	ctx := context.Background()
	done, err := r.Do(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if done {
		t.Error("should not be done")
	}
	counters := r.Counters()
	if got, want := counters["ndone"], 2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := counters["nfail"], 2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	trials, err := db.Load(study)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(trials), 0; got != want {
		t.Error("found trials")
	}
}

func runnerTest(t *testing.T) (dir string, cleanup func()) {
	dir, cleanupDir := testutil.TempDir(t, "", "")
	save := runner.Preamble
	runner.Preamble = "set -ex; "
	return dir, func() {
		cleanupDir()
		runner.Preamble = save
	}
}

var policy = retry.Backoff(time.Second, 5*time.Second, 1.5)

func eventually(cond func() bool) bool {
	for try := 0; try < 10; try++ {
		if cond() {
			return true
		}
		if err := retry.Wait(context.Background(), policy, try); err != nil {
			panic(err)
		}
	}
	return cond()
}
