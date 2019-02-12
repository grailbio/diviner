// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package script_test

import (
	"reflect"
	"testing"

	"github.com/grailbio/diviner"
	"github.com/grailbio/diviner/oracle"
	"github.com/grailbio/diviner/script"
)

func TestScript(t *testing.T) {
	studies, config, err := script.Load("testdata/simple.diviner", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := config, (script.Config{Database: script.Local, Table: "test"}); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := len(studies), 2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := studies[0].Name, "study_1"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := studies[1].Name, "study_2"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := studies[0].Objective, (diviner.Objective{diviner.Minimize, "x"}); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := studies[1].Objective, (diviner.Objective{diviner.Maximize, "z"}); got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	params := diviner.Params{
		"learning_rate": diviner.NewDiscrete(diviner.Float(0.1), diviner.Float(0.2), diviner.Float(0.3)),
		"dropout":       diviner.NewDiscrete(diviner.Float(0.5), diviner.Float(0.8)),
	}
	if got, want := studies[0].Params, params; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := studies[0].Oracle, (&oracle.Skopt{AcquisitionFunc: "EI"}); !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	runConfig := studies[0].Run(diviner.Values{
		"learning_rate": diviner.Float(0.1),
		"dropout":       diviner.Float(0.5),
	})
	expect := diviner.RunConfig{
		Script:     "echo 0.1 0.5",
		LocalFiles: []string{"x", "y", "z"},
	}
	if got, want := runConfig, expect; !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}
