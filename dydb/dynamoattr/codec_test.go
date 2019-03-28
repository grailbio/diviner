// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package dynamoattr

import (
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	fuzz "github.com/google/gofuzz"
)

type test struct {
	Bool   bool `dynamoattr:"a_bool"`
	String string
	Uint8  uint8
	Int    int
	List   []string
	Map    map[string]int
	Ignore int `dynamoattr:"-"`
	Bytes  []byte
}

func TestUnmarshal(t *testing.T) {
	attrs := map[string]*dynamodb.AttributeValue{
		"Bool":   {BOOL: aws.Bool(true)},
		"a_bool": {BOOL: aws.Bool(false)},
		"String": {S: aws.String("hello world")},
		"Uint8":  {N: aws.String("89")},
		"Int":    {N: aws.String("123456")},
		"List":   {L: []*dynamodb.AttributeValue{{S: aws.String("a")}, {S: aws.String("b")}}},
		"Ignore": {N: aws.String("321")},
	}

	var val test
	if err := Unmarshal(attrs, &val); err != nil {
		t.Fatal(err)
	}
	// Trigger the cache:
	if err := Unmarshal(attrs, &val); err != nil {
		t.Fatal(err)
	}
	if got, want := val, (test{false, "hello world", 89, 123456, []string{"a", "b"}, nil, 0, nil}); !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestRoundtrip(t *testing.T) {
	const N = 1000
	examples := make([]test, N)
	fz := fuzz.New()
	fz.Fuzz(&examples)
	for _, example := range examples {
		example.Ignore = 0
		attrs, err := Marshal(example)
		if err != nil {
			t.Fatalf("failed to marshal %v", example)
		}
		var got test
		if err := Unmarshal(attrs, &got); err != nil {
			t.Fatalf("failed to unmarshal %v", example)
		}
		if got, want := got, example; !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}
