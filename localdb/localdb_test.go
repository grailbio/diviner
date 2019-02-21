// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package localdb_test

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/grailbio/diviner"
	"github.com/grailbio/diviner/localdb"
	"github.com/grailbio/testutil"
)

func TestDB(t *testing.T) {
	dir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	ctx := context.Background()
	const N = 1000

	db, err := localdb.Open(filepath.Join(dir, "test.ddb"))
	if err != nil {
		t.Fatal(err)
	}

	study := diviner.Study{
		Name: "test1",
		Params: diviner.Params{
			"index":         diviner.NewRange(diviner.Int(0), diviner.Int(N)),
			"learning_rate": diviner.NewRange(diviner.Float(0.1), diviner.Float(1.0)),
			"dropout":       diviner.NewRange(diviner.Float(0.01), diviner.Float(0.1)),
		},
		Objective: diviner.Objective{diviner.Maximize, "acc"},
	}

	values := diviner.Values{"index": diviner.Int(0), "learning_rate": diviner.Float(0.5), "dropout": diviner.Float(0.05)}
	run, err := db.New(ctx, study, values)
	if err != nil {
		t.Fatal(err)
	}

	run2, err := db.Run(ctx, run.ID())
	if err != nil {
		t.Fatal(err)
	}
	if got, want := run2, run; !runEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	_, err = run2.Trial(ctx)
	if got, want := err, localdb.ErrIncompleteRun; got != want {
		t.Fatal(err)
	}
	if got, want := run2.State(), diviner.Pending; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	for _, acc := range []float64{0.5, 0.67, 0} {
		if err := run2.Update(ctx, diviner.Metrics{"acc": acc}); err != nil {
			t.Fatal(err)
		}
	}
	trial, err := run2.Trial(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := trial.Values, values; !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := trial.Metrics, (diviner.Metrics{"acc": 0}); !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	runs, err := db.Runs(ctx, study, diviner.Pending|diviner.Complete)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(runs), 1; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := runs[0], run2; !runEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	for i := 1; i < N; i++ {
		run, err := db.New(ctx, study, diviner.Values{"index": diviner.Int(i), "learning_rate": diviner.Float(0.5), "dropout": diviner.Float(0.05)})
		if err != nil {
			t.Fatal(err)
		}
		if err := run.Update(ctx, diviner.Metrics{"acc": float64(i)}); err != nil {
			t.Fatal(err)
		}
	}

	runs, err = db.Runs(ctx, study, diviner.Pending|diviner.Complete)
	if got, want := len(runs), N; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	for _, run := range runs {
		if err := run.Complete(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for i, run := range runs {
		trial, err := run.Trial(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if run.State() != diviner.Complete {
			t.Error("wanted complete")
		}
		if got, want := trial.Metrics["acc"], float64(i); got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}

	const Size = 1 << 20
	var (
		n int
		b bytes.Buffer
	)
	run = runs[0]
	for n < Size {
		m := rand.Intn(Size)
		if m > n+Size {
			m = Size - n
		}
		p := make([]byte, m)
		if _, err := rand.Read(p); err != nil {
			t.Fatal(err)
		}
		if _, err := io.Copy(run, bytes.NewReader(p)); err != nil {
			t.Fatal(err)
		}
		if _, err := io.Copy(&b, bytes.NewReader(p)); err != nil {
			t.Fatal(err)
		}
		n += m
	}
	if err := run.Flush(); err != nil {
		t.Fatal(err)
	}
	var c bytes.Buffer
	if _, err := io.Copy(&c, run.Log()); err != nil {
		t.Fatal(err)
	}
	if got, want := c.Len(), b.Len(); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if !bytes.Equal(b.Bytes(), c.Bytes()) {
		t.Error("log diff")
	}
}

func runEqual(r, u diviner.Run) bool {
	type eq interface {
		Equal(r diviner.Run) bool
	}
	return r.(eq).Equal(u)
}