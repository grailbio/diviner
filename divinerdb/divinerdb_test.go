// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package divinerdb_test

import (
	"path/filepath"
	"reflect"
	"testing"

	"github.com/grailbio/diviner"
	"github.com/grailbio/diviner/divinerdb"
	"github.com/grailbio/testutil"
)

func TestDivinerDB(t *testing.T) {
	dir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	db := divinerdb.New(filepath.Join(dir, "diviner."))
	defer db.Close()
	study := diviner.Study{
		Name: "test1",
		Params: diviner.Params{
			"learning_rate": diviner.NewDiscrete(diviner.Float(0.1), diviner.Float(0.5), diviner.Float(1.0)),
			"dropout":       diviner.NewDiscrete(diviner.Float(0.5), diviner.Float(0.8)),
			"optimizer":     diviner.NewDiscrete(diviner.String("comma,"), diviner.String("s,e<-#!mi;colon")),
		},
		Objective: diviner.Objective{diviner.Maximize, "acc"},
	}
	params := []struct{ learningRate, dropout, loss, acc float64 }{
		{0.1, 0.5, 1.0, 0.5},
		{0.2, 0.5, 2.0, 0.3},
		{0.1, 0.5, 2.4, 0.3},
	}
	trials := make([]diviner.Trial, len(params))
	for i, p := range params {
		trials[i] = diviner.Trial{
			Values: diviner.Values{
				"learning_rate": diviner.Float(p.learningRate),
				"dropout":       diviner.Float(p.dropout),
			},
			Metrics: map[string]float64{
				"loss": p.loss,
				"acc":  p.acc,
			},
		}
	}
	for _, trial := range trials {
		if err := db.Report(study, trial); err != nil {
			t.Fatal(err)
		}
	}
	reported, err := db.Load(study)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := reported, trials; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}
