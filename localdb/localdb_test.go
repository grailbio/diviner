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
	"time"

	"github.com/grailbio/diviner"
	"github.com/grailbio/diviner/localdb"
	"github.com/grailbio/testutil"
)

func TestDB(t *testing.T) {
	dir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	ctx := context.Background()
	const N = 100

	db, err := localdb.Open(filepath.Join(dir, "test.ddb"))
	if err != nil {
		t.Fatal(err)
	}

	studies, err := db.Studies(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(studies), 0; got != want {
		t.Errorf("got %v, want %v", got, want)
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
	run, err := db.New(ctx, study, values, diviner.RunConfig{})
	if err != nil {
		t.Fatal(err)
	}

	run2, err := db.Run(ctx, "test1", run.ID())
	if err != nil {
		t.Fatal(err)
	}
	if got, want := run2, run; !runEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	metrics, err := run2.Metrics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(metrics) != 0 {
		t.Fatal("reported metrics")
	}
	if got, want := run2.State(), diviner.Pending; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	for _, acc := range []float64{0.5, 0.67, 0} {
		if err := run2.Update(ctx, diviner.Metrics{"acc": acc}); err != nil {
			t.Fatal(err)
		}
	}
	metrics, err = run2.Metrics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := metrics, (diviner.Metrics{"acc": 0}); !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	runs, err := db.Runs(ctx, study.Name, diviner.Any)
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
		var (
			values = diviner.Values{"index": diviner.Int(i), "learning_rate": diviner.Float(0.5), "dropout": diviner.Float(0.05)}
			config = diviner.RunConfig{}
		)
		run, err := db.New(ctx, study, values, config)
		if err != nil {
			t.Fatal(err)
		}
		if err := run.Update(ctx, diviner.Metrics{"acc": float64(i)}); err != nil {
			t.Fatal(err)
		}
	}

	runs, err = db.Runs(ctx, study.Name, diviner.Any)
	if got, want := len(runs), N; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	for _, run := range runs {
		if err := run.Complete(ctx, diviner.Success, time.Second); err != nil {
			t.Fatal(err)
		}
	}
	for i, run := range runs {
		metrics, err := run.Metrics(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if run.State() != diviner.Success {
			t.Error("wanted complete")
		}
		if got, want := metrics["acc"], float64(i); got != want {
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
	if _, err := io.Copy(&c, run.Log(false)); err != nil {
		t.Fatal(err)
	}
	if got, want := c.Len(), b.Len(); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if !bytes.Equal(b.Bytes(), c.Bytes()) {
		t.Error("log diff")
	}

	studies, err = db.Studies(ctx, "xxx")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(studies), 0; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	studies, err = db.Studies(ctx, "test")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(studies), 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func runEqual(r, u diviner.Run) bool {
	type eq interface {
		Equal(r diviner.Run) bool
	}
	return r.(eq).Equal(u)
}
