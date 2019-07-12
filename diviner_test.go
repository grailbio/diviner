// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package diviner

import (
	"testing"
	"time"
)

func TestReplicates(t *testing.T) {
	var r Replicates
	r.Set(2)
	if !r.Contains(2) {
		t.Error("not contains 3")
	}
	if r.Completed(3) {
		t.Error("complete 3")
	}
	r.Set(0)
	r.Set(1)
	if !r.Completed(3) {
		t.Error("not complete 3")
	}
	r.Clear(0)
	if r.Completed(3) {
		t.Error("complete 3")
	}
	if got, want := r.Count(), 2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	seen := make(map[int]bool)
	for num, r := r.Next(); num != -1; num, r = r.Next() {
		if seen[num] {
			t.Errorf("seen twice: %d", num)
		}
		seen[num] = true
	}
	if got, want := len(seen), 2; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if !seen[1] || !seen[2] {
		t.Errorf("not seen 1, 2: %v", seen)
	}
}

func TestReplicatedTrial(t *testing.T) {
	rep := replicatedTrial(
		Run{Replicate: 0, State: Success, Metrics: []Metrics{{"x": 1.0}}},
		Run{Replicate: 0, State: Pending, Metrics: []Metrics{{"x": 0.5}}},
		Run{Replicate: 1, State: Success, Metrics: []Metrics{{"x": 2.0}}},
	)
	if got, want := rep.Metrics, (Metrics{"x": 1.5}); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := rep.Replicates.Count(), 2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if rep.Pending {
		t.Error("rep was pending")
	}

	var (
		now  = time.Now()
		then = now.Add(-time.Minute)
	)
	rep = replicatedTrial(
		Run{Replicate: 0, State: Pending, Updated: then, Metrics: []Metrics{{"x": 1.0}}},
		Run{Replicate: 0, State: Pending, Updated: now, Metrics: []Metrics{{"x": 0.5}}},
		Run{Replicate: 1, State: Success, Metrics: []Metrics{{"x": 2.0}}},
	)
	if got, want := rep.Metrics, (Metrics{"x": 1.25}); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := rep.Replicates.Count(), 2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if !rep.Pending {
		t.Error("rep was not pending")
	}
}

func replicatedTrial(runs ...Run) Trial {
	trials := make([]Trial, len(runs))
	for i, run := range runs {
		trials[i] = run.Trial()
	}
	return ReplicatedTrial(trials)
}
