// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package dynamoattr provides functions for marshaling and
// unmarshaling Go values to and from DynamoDB items
// (map[string]*dynamodb.AttributeValue).
//
// The mapping between DynamodDB items and Go values is described in
// the Marshal and Unmarshal functions.
package dynamoattr

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var mappingCache sync.Map // map[reflect.Type]map[string]attr

// Marshal returns a DynamoDB attribute map for the Go struct v.
//
// Marshal traverses v recursively, encoding its structure into the
// returned attribute map. Marshal returns an error if it encounters
// a Go value that is incompatible with the attribute map data model.
//
// The toplevel value v must be a struct: its fields are mapped into
// the toplevel attribute names. Struct attributes are named by their
// Go field name. This mapping can be overridden by specifying a
// field tag under the "dynamoattr" key. As a special case, fields
// renamed to "-" are ignored by Marshal. Structs are not permitted
// beyond the top level.
//
// Unexported fields are skipped.
func Marshal(v interface{}) (map[string]*dynamodb.AttributeValue, error) {
	mapping, err := mappingOf(reflect.TypeOf(v))
	if err != nil {
		return nil, err
	}
	var (
		val   = reflect.ValueOf(v)
		attrs = make(map[string]*dynamodb.AttributeValue)
	)
	for tag, field := range mapping {
		attr, err := encode(val.Field(field))
		if err != nil {
			return nil, fmt.Errorf("error encoding attribute %s: %v", tag, err)
		}
		if attr != nil {
			attrs[tag] = attr
		}
	}
	return attrs, nil
}

func encode(val reflect.Value) (*dynamodb.AttributeValue, error) {
	attr := new(dynamodb.AttributeValue)
	switch val.Kind() {
	case reflect.Bool:
		attr.BOOL = aws.Bool(val.Bool())
	case reflect.String:
		if val.String() == "" {
			// The AWS APIs do not accept empty string attributes.
			return nil, nil
		}
		attr.S = aws.String(val.String())
	case reflect.Int8, reflect.Int32, reflect.Int64, reflect.Int:
		attr.N = aws.String(strconv.FormatInt(val.Int(), 10))
	case reflect.Uint8, reflect.Uint32, reflect.Uint64, reflect.Uint:
		attr.N = aws.String(strconv.FormatUint(val.Uint(), 10))
	case reflect.Float32, reflect.Float64:
		attr.N = aws.String(strconv.FormatFloat(val.Float(), 'f', -1, val.Type().Bits()))
	case reflect.Map:
		if val.Type().Key().Kind() != reflect.String {
			return nil, fmt.Errorf("unsupported type %s", val.Type())
		}
		if val.IsNil() {
			return nil, nil
		}
		attr.M = make(map[string]*dynamodb.AttributeValue, val.Len())
		for iter := val.MapRange(); iter.Next(); {
			var err error
			attr.M[iter.Key().String()], err = encode(iter.Value())
			if err != nil {
				return nil, err
			}
		}
	case reflect.Slice:
		switch val.Type().Elem().Kind() {
		default:
			if val.IsNil() {
				return nil, nil
			}
			attr.L = make([]*dynamodb.AttributeValue, val.Len())
			for i := range attr.L {
				var err error
				attr.L[i], err = encode(val.Index(i))
				if err != nil {
					return nil, err
				}
			}
		case reflect.Uint8:
			if val.IsNil() {
				return nil, nil
			}
			attr.B = make([]byte, val.Len())
			copy(attr.B, val.Interface().([]byte))
		}
	default:
		return nil, fmt.Errorf("unsupported type %s", val.Type())
	}
	return attr, nil
}

// Unmarshal decodes an attribute map into the Go value ptr, which
// must be a pointer to a toplevel struct, compatible with the
// provided attribute mapping.
//
// Attribute maps are decoded in the manner described by Marshal.
// Unmarshal returns an error If an attribute map is incompatible
// with the provided struct.
func Unmarshal(attrs map[string]*dynamodb.AttributeValue, ptr interface{}) error {
	typ := reflect.TypeOf(ptr)
	if typ.Kind() != reflect.Ptr || typ.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("expected pointer to struct, got %s", typ)
	}
	mapping, err := mappingOf(typ.Elem())
	if err != nil {
		return err
	}
	val := reflect.ValueOf(ptr).Elem()
	for tag, attr := range attrs {
		field, ok := mapping[tag]
		if !ok {
			continue
		}
		if err := decode(attr, val.Field(field)); err != nil {
			return fmt.Errorf("error decoding attribute %s: %v", tag, err)
		}
	}
	return nil
}

func decode(attr *dynamodb.AttributeValue, val reflect.Value) error {
	switch val.Kind() {
	case reflect.Bool:
		if attr.BOOL == nil {
			return errors.New("attribute is not of type BOOL")
		}
		val.SetBool(*attr.BOOL)
	case reflect.String:
		// DynamoDB does not permit empty strings, so we allow
		// these to be nil.
		if attr == nil {
			val.SetString("")
		} else {
			val.SetString(aws.StringValue(attr.S))
		}
	case reflect.Int8, reflect.Int32, reflect.Int64, reflect.Int:
		if attr.N == nil {
			return errors.New("attribute is not of type NUMBER")
		}
		v, err := strconv.ParseInt(*attr.N, 0, val.Type().Bits())
		if err != nil {
			return err
		}
		val.SetInt(v)
	case reflect.Uint8, reflect.Uint32, reflect.Uint64, reflect.Uint:
		if attr.N == nil {
			return errors.New("attribute is not of type NUMBER")
		}
		v, err := strconv.ParseUint(*attr.N, 0, val.Type().Bits())
		if err != nil {
			return err
		}
		val.SetUint(v)
	case reflect.Float32, reflect.Float64:
		if attr.N == nil {
			return errors.New("attribute is not of type NUMBER")
		}
		v, err := strconv.ParseFloat(*attr.N, val.Type().Bits())
		if err != nil {
			return err
		}
		val.SetFloat(v)
	case reflect.Map:
		if attr.M == nil {
			return errors.New("attribute is not of type MAP")
		}
		if val.Type().Key().Kind() != reflect.String {
			return errors.New("map key has wrong type: only keys of type STRING are allowed")
		}
		val.Set(reflect.MakeMapWithSize(val.Type(), len(attr.M)))
		for key, attr := range attr.M {
			// TODO(marius): is there a better way to do this?
			v := reflect.New(val.Type().Elem())
			if err := decode(attr, v.Elem()); err != nil {
				return err
			}
			val.SetMapIndex(reflect.ValueOf(key), v.Elem())
		}
	case reflect.Slice:
		switch val.Type().Elem().Kind() {
		default:
			if attr.L == nil {
				return errors.New("attribute is not of type LIST")
			}
			val.Set(reflect.MakeSlice(val.Type(), len(attr.L), len(attr.L)))
			for i := range attr.L {
				if err := decode(attr.L[i], val.Index(i)); err != nil {
					return err
				}
			}
		case reflect.Uint8: // []byte
			if attr.B == nil {
				return errors.New("attribute is not of type BINARY")
			}
			val.Set(reflect.ValueOf(attr.B))
		}
	default:
		return fmt.Errorf("unsupported type %s", val.Type())
	}
	return nil
}

func mappingOf(typ reflect.Type) (map[string]int, error) {
	if attrs, ok := mappingCache.Load(typ); ok {
		return attrs.(map[string]int), nil
	}
	attrs := make(map[string]int)
	for i := 0; i < typ.NumField(); i++ {
		var (
			f   = typ.Field(i)
			tag = f.Tag.Get("dynamoattr")
		)
		// Ignore unexported fields.
		if f.PkgPath != "" {
			if tag != "" {
				return nil, fmt.Errorf("dynamoattr tag specified on unexported field %s", f.Name)
			}
			continue
		}
		switch tag {
		case "-":
			continue
		case "":
			tag = f.Name
		}
		attrs[tag] = i
	}
	mappingCache.Store(typ, attrs)
	return attrs, nil
}
