// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package divinerdb provides a simple database of trials
// performed by studies.
//
// TODO(marius): "database" is a bit of a misnomer.
package divinerdb

import (
	"bufio"
	"fmt"
	"log"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/diviner"
)

// A DB is a database that stores trails as performed by studies.
// It is used to log trials and retrieve past results.
type DB struct {
	prefix string

	mu      sync.Mutex
	studies map[string]*os.File
}

// New returns a new DB that stores trials on the filesystem
// with the provided prefix.
func New(prefix string) *DB {
	return &DB{
		prefix:  prefix,
		studies: make(map[string]*os.File),
	}
}

// Close closes all open files held by this DB instance.
func (d *DB) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	var err errors.Once
	for _, file := range d.studies {
		err.Set(file.Close())
	}
	return err.Err()
}

// Load loads all trials associated with the provided study.
func (d *DB) Load(study diviner.Study) ([]diviner.Trial, error) {
	// Do this under lock so that we don't append elements while also
	// loading the current values.
	d.mu.Lock()
	defer d.mu.Unlock()
	f, err := os.Open(d.path(study))
	if err != nil {
		if os.IsNotExist(err) {
			err = nil
		}
		return nil, err
	}
	var (
		scan   = bufio.NewScanner(f)
		trials []diviner.Trial
		lineno int
		params = study.Params
	)
	for scan.Scan() {
		parts := strings.Split(scan.Text(), "|")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid record on line %d: %s", lineno, scan.Text())
		}
		var trial diviner.Trial
		elems := strings.Split(parts[0], ",")
		trial.Values = make(diviner.Values)
		for _, elem := range elems {
			parts := strings.SplitN(elem, "=", 2)
			if len(parts) != 2 {
				return nil, fmt.Errorf("invalid value on line %d: %s", lineno, elem)
			}
			key := parts[0]
			param, ok := params[key]
			if !ok {
				log.Printf("skipping stored param %s not included in study", key)
				continue
			}
			var val diviner.Value
			switch param.Kind() {
			case diviner.Integer:
				v64, err := strconv.ParseInt(parts[1], 10, 64)
				if err != nil {
					return nil, fmt.Errorf("invalid integer value %s on line %d: %v", parts[1], lineno, err)
				}
				val = diviner.Int(v64)
			case diviner.Real:
				v64, err := strconv.ParseFloat(parts[1], 64)
				if err != nil {
					return nil, fmt.Errorf("invalid float value %s on line %d: %v", parts[1], lineno, err)
				}
				val = diviner.Float(v64)
			case diviner.Str:
				str, err := url.QueryUnescape(parts[1])
				if err != nil {
					return nil, fmt.Errorf("invalid string value %s on line %d: %v", parts[1], lineno, err)
				}
				val = diviner.String(str)
			}
			trial.Values[parts[0]] = val
		}
		elems = strings.Split(parts[1], ",")
		trial.Metrics = make(map[string]float64)
		for _, elem := range elems {
			if elem == "" {
				continue
			}
			parts := strings.SplitN(elem, "=", 2)
			if len(parts) != 2 {
				return nil, fmt.Errorf("invalid metric on line %d: %s", lineno, elem)
			}
			val, err := strconv.ParseFloat(parts[1], 64)
			if err != nil {
				return nil, fmt.Errorf("invalid metric value %s (%s) on line %d: %v", parts[0], parts[1], lineno, err)
			}
			trial.Metrics[parts[0]] = val
		}
		trials = append(trials, trial)
		lineno++
	}
	return trials, nil
}

// Report reports the provided trial under the provided study; the results
// are added to the database.
func (d *DB) Report(study diviner.Study, trial diviner.Trial) error {
	keys := make([]string, 0, len(trial.Values))
	for key := range trial.Values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	valueElems := make([]string, len(keys))
	for i, key := range keys {
		var str string
		switch val := trial.Values[key]; val.Kind() {
		case diviner.Integer:
			str = strconv.FormatInt(val.Int(), 10)
		case diviner.Real:
			str = strconv.FormatFloat(val.Float(), 'f', -1, 64)
		case diviner.Str:
			str = url.QueryEscape(val.Str())
		}
		valueElems[i] = fmt.Sprintf("%s=%s", key, str)
	}
	keys = keys[:0]
	for key := range trial.Metrics {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	metricElems := make([]string, len(keys))
	for i, key := range keys {
		metricElems[i] = fmt.Sprintf("%s=%f", key, trial.Metrics[key])
	}
	var b strings.Builder
	b.WriteString(strings.Join(valueElems, ","))
	b.WriteString("|")
	b.WriteString(strings.Join(metricElems, ","))
	d.mu.Lock()
	defer d.mu.Unlock()
	f, err := d.openLocked(study)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(f, b.String())
	_ = f.Sync()
	return err
}

func (d *DB) path(study diviner.Study) string {
	return fmt.Sprintf("%s%s.trials", d.prefix, study.Name)
}

func (d *DB) openLocked(study diviner.Study) (*os.File, error) {
	f := d.studies[study.Name]
	if f != nil {
		return f, nil
	}
	var err error
	d.studies[study.Name], err = os.OpenFile(d.path(study), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	return d.studies[study.Name], err
}
