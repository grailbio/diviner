// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package localdb_test

import (
	"context"
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

	studies, err := db.ListStudies(ctx, "", time.Time{})
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

	created, err := db.CreateStudyIfNotExist(ctx, study)
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("expected created")
	}
	created, err = db.CreateStudyIfNotExist(ctx, study)
	if err != nil {
		t.Fatal(err)
	}
	if created {
		t.Fatal("expected not created")
	}

	studies, err = db.ListStudies(ctx, "", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(studies), 1; got != want {
		t.Fatal(err)
	}
	if got, want := studies[0], study; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	run := diviner.Run{
		Study:  "test1",
		Values: diviner.Values{"index": diviner.Int(0), "learning_rate": diviner.Float(0.5), "dropout": diviner.Float(0.05)},
	}
	inserted, err := db.InsertRun(ctx, run)
	if err != nil {
		t.Fatal(err)
	}
	if inserted.Updated.IsZero() {
		t.Error("zero update time")
	}

	run, err = db.LookupRun(ctx, "test1", inserted.Seq)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := run.Values, inserted.Values; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := run.Study, inserted.Study; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := run.Seq, inserted.Seq; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := run.Metrics, inserted.Metrics; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	accMetrics := []float64{0.5, 0.67, 0}
	for _, acc := range accMetrics {
		if err := db.AppendRunMetrics(ctx, run.Study, run.Seq, diviner.Metrics{"acc": acc}); err != nil {
			t.Fatal(err)
		}
	}
	// If we look them up, they should all be there now.
	run, err = db.LookupRun(ctx, run.Study, run.Seq)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(run.Metrics), len(accMetrics); got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i, val := range accMetrics {
		if got, want := run.Metrics[i]["acc"], val; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}

	runs, err := db.ListRuns(ctx, study.Name, diviner.Any, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(runs), 1; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := runs[0], run; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

/*

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

	studies, err = db.Studies(ctx, "xxx", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(studies), 0; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	studies, err = db.Studies(ctx, "test", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(studies), 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
*/
