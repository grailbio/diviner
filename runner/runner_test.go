// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package runner_test

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/grailbio/base/retry"
	"github.com/grailbio/bigmachine"
	"github.com/grailbio/bigmachine/testsystem"
	"github.com/grailbio/diviner"
	"github.com/grailbio/diviner/localdb"
	"github.com/grailbio/diviner/oracle"
	"github.com/grailbio/diviner/runner"
	"github.com/grailbio/testutil"
)

// failingSystem implements bigmachine.System. Start always fails.
type errorSystem struct {
	*testsystem.System
}

func (s *errorSystem) Start(_ context.Context, count int) ([]*bigmachine.Machine, error) {
	fmt.Print("starterror")
	return nil, fmt.Errorf("error system does not support start")
}

func init() {
	gob.Register(new(errorSystem))
	runner.TestSetRetryBackoff(1 * time.Second)
}

func TestRunner(t *testing.T) {
	dir, db, cleanup := runnerTest(t)
	defer cleanup()
	ctx := context.Background()

	test := testsystem.New()
	systems := []*diviner.System{
		&diviner.System{ID: "error", System: &errorSystem{testsystem.New()}},
		&diviner.System{ID: "test", System: test},
	}

	datasetFile := filepath.Join(dir, "dataset")
	dataset := diviner.Dataset{
		Name:    "testset",
		Systems: systems,
		Script: fmt.Sprintf(`
			# Should run only once.
			test -f %s && exit 1
			echo ran > %s
		`, datasetFile, datasetFile),
	}

	study := diviner.Study{
		Name: "test",
		Params: diviner.Params{
			"param": diviner.NewDiscrete(diviner.Int(0), diviner.Int(1), diviner.Int(2)),
		},
		Run: func(values diviner.Values, id string) (diviner.RunConfig, error) {
			return diviner.RunConfig{
				Systems:  systems,
				Datasets: []diviner.Dataset{dataset},
				Script: fmt.Sprintf(`
						# Dataset should have been produced.
						test -f %s || exit 1
						echo hello world
						echo METRICS: paramvalue=1
						echo METRICS: another=3,paramvalue=%s
					`, datasetFile, values["param"]),
			}, nil
		},
		Objective: diviner.Objective{diviner.Maximize, "acc"},
		Oracle:    &oracle.GridSearch{},
	}
	if done := testRun(t, db, study); !done {
		t.Fatal("not done")
	}
	runs, err := db.ListRuns(ctx, study.Name, diviner.Success, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	trials := make([]diviner.Trial, len(runs))
	for i, run := range runs {
		trials[i] = run.Trial()
	}
	sort.Slice(trials, func(i, j int) bool {
		return trials[i].Values["param"].Int() < trials[j].Values["param"].Int()
	})
	expect := make([]diviner.Trial, 3)
	for i := range expect {
		expect[i] = diviner.Trial{
			Values:  diviner.Values{"param": diviner.Int(i)},
			Metrics: diviner.Metrics{"paramvalue": float64(i), "another": 3},
		}
	}
	if got, want := trials, expect; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	// Make sure the machines are stopped.
	for _, m := range test.B().Machines() {
		if !eventually(func() bool { return m.Err() != nil }) {
			t.Errorf("machine %v did not stop", m)
		}
	}
}

func TestRunnerError(t *testing.T) {
	_, db, cleanup := runnerTest(t)
	defer cleanup()
	ctx := context.Background()

	test := testsystem.New()
	systems := []*diviner.System{&diviner.System{ID: "test", System: test}}
	dataset := diviner.Dataset{
		Systems: systems,
		Name:    "testset",
		Script:  "exit 1",
	}
	study := diviner.Study{
		Name: "test",
		Params: diviner.Params{
			"param": diviner.NewDiscrete(diviner.Int(0), diviner.Int(1)),
		},
		Run: func(values diviner.Values, id string) (diviner.RunConfig, error) {
			config := diviner.RunConfig{
				Systems: systems,
				Script:  "echo the_status; exit 1",
			}
			if values["param"].Int() == 0 {
				// In this case, the run should fail before attempting
				// execution.
				config.Datasets = []diviner.Dataset{dataset}
			}
			return config, nil
		},
		Objective: diviner.Objective{diviner.Maximize, "acc"},
		Oracle:    &oracle.GridSearch{},
	}
	if done := testRun(t, db, study); done {
		t.Fatal("should not be done")
	}

	runs, err := db.ListRuns(ctx, study.Name, diviner.Success, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(runs), 0; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	runs, err = db.ListRuns(ctx, study.Name, diviner.Failure, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(runs), 2; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	for _, run := range runs {
		if run.Values["param"].Int() == 0 {
			continue
		}
		var b bytes.Buffer
		if _, err := io.Copy(&b, db.Log(run.Study, run.Seq, false)); err != nil {
			t.Error(err)
			continue
		}
		lines := strings.Split(b.String(), "\n")
		if got, want := len(lines), 5; got != want {
			t.Fatalf("got %v, want %v", got, want)
		}
		if !strings.HasPrefix(lines[0], "diviner: ") {
			t.Errorf("bad lines[0]: %v", lines[0])
		}
		expect := []string{
			"+ echo the_status",
			"the_status",
			"+ exit 1",
			"",
		}
		for i := range expect {
			if got, want := lines[i+1], expect[i]; got != want {
				t.Errorf("bad line %d: got %v, want %v", i+1, got, want)
			}
		}
	}
}

func TestKeepalive(t *testing.T) {
	_, db, cleanup := runnerTest(t)
	defer cleanup()

	r := runner.New(db)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		if err := r.Loop(ctx); err != context.Canceled {
			t.Fatal(err)
		}
	}()

	testScript(t, db, 5, diviner.Failure, `
echo 'DIVINER: keepalive=1ms'
sleep 1
`)

	testScript(t, db, 2, diviner.Success, `
if [ $DIVINER_TEST_COUNT -lt 2 ]
then
	echo 'DIVINER: keepalive=1ms'
	sleep 1
else
	sleep 1
fi
`)

	testScript(t, db, 0, diviner.Success, `
echo 'DIVINER: keepalive=2s'
sleep 1
echo 'DIVINER: keepalive=3s'
sleep 2
`)

	// Regular failures should not be retried.
	testScript(t, db, 0, diviner.Failure, `exit 1`)
}

func runnerTest(t *testing.T) (dir string, database diviner.Database, cleanup func()) {
	t.Helper()
	dir, cleanup = testutil.TempDir(t, "", "")
	var err error
	database, err = localdb.Open(filepath.Join(dir, "test.ddb"))
	if err != nil {
		t.Fatal(err)
	}
	return dir, database, cleanup
}

func testScript(t *testing.T, db diviner.Database, retries int, state diviner.RunState, script string) {
	t.Helper()
	r := runner.New(db)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		if err := r.Loop(ctx); err != context.Canceled {
			t.Fatal(err)
		}
	}()
	study := testStudy(script)
	run, err := r.Run(ctx, study, nil)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := run.Retries, retries; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := run.State, state; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	return
}

func testStudy(script string) diviner.Study {
	test := testsystem.New()
	systems := []*diviner.System{&diviner.System{ID: "test", System: test}}
	return diviner.Study{
		Name: "test",
		Params: diviner.Params{
			"param": diviner.NewDiscrete(diviner.Int(0), diviner.Int(1)),
		},
		Run: func(values diviner.Values, id string) (diviner.RunConfig, error) {
			config := diviner.RunConfig{
				Systems: systems,
				Script:  script,
			}
			return config, nil
		},
		Objective: diviner.Objective{diviner.Maximize, "acc"},
		Oracle:    &oracle.GridSearch{},
	}
}

func testRun(t *testing.T, db diviner.Database, study diviner.Study) (done bool) {
	t.Helper()
	r := runner.New(db)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		if err := r.Loop(ctx); err != context.Canceled {
			t.Fatal(err)
		}
	}()
	var err error
	done, err = r.Round(ctx, study, 0)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	return
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
