// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package script_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/grailbio/bigmachine"
	"github.com/grailbio/diviner"
	"github.com/grailbio/diviner/oracle"
	"github.com/grailbio/diviner/script"
)

func TestScript(t *testing.T) {
	studies, err := script.Load("testdata/simple.diviner", nil)
	if err != nil {
		t.Fatal(err)
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

	runConfig, err := studies[0].Run(diviner.Values{
		"learning_rate": diviner.Float(0.1),
		"dropout":       diviner.Float(0.5),
	}, "testrun")
	if err != nil {
		t.Fatal(err)
	}
	expect := diviner.RunConfig{
		Script:     "echo 0.1 0.5",
		LocalFiles: []string{"x", "y", "z"},
		Systems: []*diviner.System{
			{System: bigmachine.Local, ID: "local"},
			{System: bigmachine.Local, ID: "local2"},
		},
	}
	if got, want := runConfig, expect; !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestScriptIdent(t *testing.T) {
	studies, err := script.Load("testdata/ident.dv", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(studies), 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	study := studies[0]
	config, err := study.Run(nil, "test1")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := config.Script, "test1"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestCommand(t *testing.T) {
	studies, err := script.Load("testdata/commands.dv", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(studies), 3; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := studies[0].Name, "foo"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := studies[1].Name, "foofoo"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := studies[2].Name, "1.4142135623730951"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestLoad(t *testing.T) {
	studies, err := script.Load("testdata/load.dv", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(studies), 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := studies[0].Name, "test"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestLoadCycle(t *testing.T) {
	_, err := script.Load("testdata/cycle.dv", nil)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "cycle in load graph involving module testdata/cycle.dv") {
		t.Fatal(err)
	}
}
