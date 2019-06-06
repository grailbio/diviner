// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package runner_test

import (
	"context"
	"math/rand"
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
					t.Fatalf("timed out while waiting for %d entries", n)
				}
			}
		}
		exit = func(n int) {
			for i := 0; i < n; i++ {
				exitc <- struct{}{}
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
	// the remainign runs.
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
