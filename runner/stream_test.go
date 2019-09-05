// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package runner_test

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/grailbio/diviner"
	"github.com/grailbio/diviner/oracle"
	"github.com/grailbio/diviner/runner"
)

func TestStream(t *testing.T) {
	_, db, cleanup := runnerTest(t)
	defer cleanup()

	var (
		enterc = make(chan struct{})
		exitc  = make(chan struct{})
		enter  = func(n int) {
			t.Helper()
			timeout := time.After(2 * time.Second)
			for i := 0; i < n; i++ {
				select {
				case <-enterc:
				case <-timeout:
					t.Fatalf("timed out while waiting (enter) for %d entries", n)
				}
			}
		}
		exit = func(n int) {
			t.Helper()
			timeout := time.After(30 * time.Second)
			for i := 0; i < n; i++ {
				select {
				case exitc <- struct{}{}:
				case <-timeout:
					t.Fatalf("timed out while waiting (exit) for %d entries", n)
				}
			}
		}
	)

	const N = 50

	study := diviner.Study{
		Name: "test",
		Params: diviner.Params{
			"param": diviner.NewRange(diviner.Int(0), diviner.Int(N)),
		},
		Acquire: func(values diviner.Values, replicate int, id string) (diviner.Metrics, error) {
			enterc <- struct{}{}
			<-exitc
			return diviner.Metrics{"acc": float64(values["param"].Int()) * 0.87}, nil
		},
		Objective: diviner.Objective{diviner.Maximize, "acc"},
		Oracle:    new(oracle.GridSearch),
	}

	r := runner.New(db)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		if err := r.Loop(ctx); err != context.Canceled {
			t.Fatal(err)
		}
	}()

	const P = 6
	streamer := r.Stream(ctx, study, P)
	var npending int
	for ntrials := 0; ntrials < N; {
		n := rand.Intn(npending + 1)
		exit(n)
		npending -= n

		max := P - npending
		if left := N - ntrials; left < max {
			max = left
		}
		n = rand.Intn(max + 1)
		enter(n)
		npending += n
		ntrials += n
	}

	// Stop the streamer first to make sure that we properly drain
	// the remaining runs.
	streamer.Stop()
	exit(npending)
	if err := streamer.Wait(); err != nil {
		t.Fatal(err)
	}

	runs, err := db.ListRuns(ctx, study.Name, diviner.Success, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(runs), N; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}

}

func TestStreamError(t *testing.T) {
	_, db, cleanup := runnerTest(t)
	defer cleanup()
	const N = 10
	var (
		fail  = true
		mu    sync.Mutex
		cond  = sync.NewCond(&mu)
		count int
		ids   = make(map[string]bool)
	)
	study := diviner.Study{
		Name: "test",
		Params: diviner.Params{
			"param": diviner.NewRange(diviner.Int(0), diviner.Int(N)),
		},
		Acquire: func(values diviner.Values, replicate int, id string) (diviner.Metrics, error) {
			mu.Lock()
			count++
			ids[id] = true
			cond.Signal()
			mu.Unlock()
			if fail {
				return nil, errors.New("failure")
			}
			return diviner.Metrics{"acc": float64(values["param"].Int()) * 0.87}, nil
		},
		Objective: diviner.Objective{diviner.Maximize, "acc"},
		Oracle:    new(oracle.GridSearch),
	}

	r := runner.New(db)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		if err := r.Loop(ctx); err != context.Canceled {
			t.Fatal(err)
		}
	}()

	streamer := r.Stream(ctx, study, 1)
	mu.Lock()
	for count == 0 {
		cond.Wait()
	}
	mu.Unlock()
	streamer.Stop()
	_ = streamer.Wait()

	trials, err := diviner.Trials(ctx, db, study, diviner.Failure)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := trials.Len(), 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	fail = false
	streamer = r.Stream(ctx, study, 3)
	mu.Lock()
	count = 0
	for count < N {
		cond.Wait()
	}
	mu.Unlock()
	streamer.Stop()
	_ = streamer.Wait()

	if got, want := len(ids), 10; got != want {
		t.Errorf("got %v, want %v", got, want)
		t.Log("ids: ", ids)
	}
	trials, err = diviner.Trials(ctx, db, study, diviner.Any)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := trials.Len(), N; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
