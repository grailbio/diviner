// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package localdb implements a diviner database on the local file
// system using boltdb.
package localdb

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/grailbio/base/log"
	"github.com/grailbio/diviner"
	bolt "go.etcd.io/bbolt"
)

const keepaliveInterval = 30 * time.Second

var (
	studiesKey = []byte("studies")
	metaKey    = []byte("meta")
	updatedKey = []byte("updated")
	runsKey    = []byte("runs")
	logsKey    = []byte("logs")
	metricsKey = []byte("metrics")
)

// DB implements diviner.Database using Bolt.
type DB struct {
	db *bolt.DB
}

// Open opens and returns a new database with the provided filename.
// The file is created if it does not already exist.
func Open(filename string) (db *DB, err error) {
	db = new(DB)
	db.db, err = bolt.Open(filename, 0666, nil)
	if err != nil {
		return nil, err
	}
	return db, db.db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucketIfNotExists(studiesKey)
		return err
	})
}

func (d *DB) CreateStudyIfNotExist(ctx context.Context, study diviner.Study) (created bool, err error) {
	err = d.db.Update(func(tx *bolt.Tx) (e error) {
		var b *bolt.Bucket
		b, created = create(tx, studiesKey, study.Name)
		if created {
			if err := put(b, updatedKey, time.Now()); err != nil {
				return err
			}
			if err := put(b, metaKey, study); err != nil {
				return err
			}
		}
		return nil
	})
	return
}

// Study implements diviner.Database.
func (d *DB) LookupStudy(ctx context.Context, name string) (study diviner.Study, err error) {
	err = d.db.View(func(tx *bolt.Tx) error {
		b := lookup(tx, studiesKey, name)
		if b == nil {
			return diviner.ErrNotExist
		}
		if ok, err := get(b, metaKey, &study); err != nil {
			return err
		} else if !ok {
			return errors.New("inconsistent database")
		}
		return nil
	})

	return
}

// Studies implements diviner.Database.
func (d *DB) ListStudies(ctx context.Context, prefix string, since time.Time) (studies []diviner.Study, err error) {
	err = d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(studiesKey)
		if b == nil {
			return nil
		}
		c := b.Cursor()
		for k, v := c.Seek([]byte(prefix)); k != nil && bytes.HasPrefix(k, []byte(prefix)); k, v = c.Next() {
			if v != nil {
				log.Error.Printf("database contains non-bucket key study key %s", k)
				continue
			}
			var updated time.Time
			if ok, err := get(b.Bucket(k), updatedKey, &updated); err != nil {
				return err
			} else if ok && updated.Before(since) {
				continue
			}
			var study diviner.Study
			if ok, err := get(b.Bucket(k), metaKey, &study); err != nil {
				return err
			} else if !ok {
				return errors.New("inconsistent database")
			}
			studies = append(studies, study)
		}
		return nil
	})
	return
}

// NextSeq reserves and returns the next sequence number for the provided study.
func (d *DB) NextSeq(ctx context.Context, study string) (seq uint64, err error) {
	err = d.db.Update(func(tx *bolt.Tx) (e error) {
		b := lookup(tx, studiesKey, study)
		if b == nil {
			return diviner.ErrNotExist
		}
		b, _ = create(b, runsKey)
		if b == nil {
			return errors.New("failed to create bucket for runs")
		}
		seq, _ = b.NextSequence()
		return nil
	})
	return
}

// New implements diviner.Database.
func (d *DB) InsertRun(ctx context.Context, run diviner.Run) (diviner.Run, error) {
	err := d.db.Update(func(tx *bolt.Tx) (e error) {
		b := lookup(tx, studiesKey, run.Study)
		if b == nil {
			return diviner.ErrNotExist
		}
		b, _ = create(b, runsKey)
		if b == nil {
			return errors.New("failed to create bucket for runs")
		}
		if run.Seq == 0 {
			run.Seq, _ = b.NextSequence()
		}
		run.Created = time.Now()
		run.Updated = run.Created
		run.State = diviner.Pending
		b, _ = create(tx, runKey{run.Study, run.Seq})
		if b == nil {
			return errors.New("failed to create bucket for run")
		}
		err := put(b, metaKey, run)
		if err := put(lookup(tx, studiesKey, run.Study), updatedKey, time.Now()); err != nil {
			log.Error.Printf("run %v: update: %s", run, err)
		}
		return err
	})
	return run, err
}

func (d *DB) UpdateRun(ctx context.Context, study string, seq uint64, state diviner.RunState, message string, runtime time.Duration, retry int) error {
	return d.db.Update(func(tx *bolt.Tx) (e error) {
		b := lookup(tx, runKey{study, seq})
		if b == nil {
			return diviner.ErrNotExist
		}
		var run diviner.Run
		ok, err := get(b, metaKey, &run)
		if err == nil && !ok {
			return diviner.ErrNotExist
		}
		run.Updated = time.Now()
		run.State = state
		run.Status = message
		run.Runtime = runtime
		run.Retries = retry
		err = put(b, metaKey, run)
		// Update the study time as well, so that it shows up properly in listings.
		if err := put(lookup(tx, studiesKey, run.Study), updatedKey, time.Now()); err != nil {
			log.Error.Printf("run %v: update: %s", run, err)
		}
		return err
	})
}

func (d *DB) AppendRunMetrics(ctx context.Context, study string, seq uint64, metrics diviner.Metrics) error {
	return d.db.Update(func(tx *bolt.Tx) (e error) {
		b := lookup(tx, runKey{study, seq})
		if b == nil {
			return diviner.ErrNotExist
		}
		b, _ = create(b, metricsKey)
		if b == nil {
			return errors.New("failed to create metrics bucket")
		}
		seq, _ := b.NextSequence()
		return put(b, seq, metrics)
	})
}

// Runs implements diviner.Database.
func (d *DB) ListRuns(ctx context.Context, study string, states diviner.RunState, since time.Time) (runs []diviner.Run, err error) {
	err = d.db.View(func(tx *bolt.Tx) error {
		b := lookup(tx, studiesKey, study)
		if b == nil {
			return diviner.ErrNotExist
		}
		b = lookup(b, runsKey)
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			if len(k) != 8 {
				return errors.New("malformed key")
			}
			var run diviner.Run
			run.Study = study
			run.Seq = binary.LittleEndian.Uint64(k)
			b := lookup(tx, runKey{study, run.Seq})
			if b == nil {
				return nil
			}
			ok, err := get(b, metaKey, &run)
			if err != nil {
				return err
			} else if err == nil && !ok {
				return nil
			}
			if run.Updated.Before(since) {
				return nil
			}
			if run.State == diviner.Pending && time.Since(run.Updated) > 2*keepaliveInterval {
				run.State = diviner.Failure
			}
			if run.State&states != run.State {
				return nil
			}
			run.Metrics, err = unmarshalMetrics(b)
			if err != nil {
				return err
			}
			runs = append(runs, run)
			return nil
		})
	})
	return
}

// Run implements diviner.Database.
func (d *DB) LookupRun(ctx context.Context, study string, seq uint64) (run diviner.Run, err error) {
	err = d.db.View(func(tx *bolt.Tx) error {
		b := lookup(tx, runKey{study, seq})
		if b == nil {
			return diviner.ErrNotExist
		}
		ok, err := get(b, metaKey, &run)
		if err == nil && !ok {
			err = diviner.ErrNotExist
		}
		if err != nil {
			return err
		}
		if run.State == diviner.Pending && time.Since(run.Updated) > 2*keepaliveInterval {
			run.State = diviner.Failure
		}
		run.Metrics, err = unmarshalMetrics(b)
		return err
	})
	return
}

type runKey struct {
	Study string
	Seq   uint64
}

type bucketer interface {
	Bucket(key []byte) *bolt.Bucket
	CreateBucket(name []byte) (*bolt.Bucket, error)
}

func create(bkt bucketer, args ...interface{}) (*bolt.Bucket, bool) {
	return bucket(bkt, true, args...)
}

func lookup(bkt bucketer, args ...interface{}) *bolt.Bucket {
	b, _ := bucket(bkt, false, args...)
	return b
}

func bucket(bkt bucketer, create bool, args ...interface{}) (*bolt.Bucket, bool) {
	if len(args) == 0 {
		panic("invalid key")
	}
	keys := make([][]byte, 0, len(args))
	for i, arg := range args {
		switch arg := arg.(type) {
		case runKey:
			if i != 0 {
				panic("run can only be the first key")
			}
			seq := make([]byte, 8)
			binary.LittleEndian.PutUint64(seq, arg.Seq)
			keys = append(keys, studiesKey, []byte(arg.Study), runsKey, seq)
		case string:
			keys = append(keys, []byte(arg))
		case []byte:
			keys = append(keys, arg)
		default:
			panic(fmt.Sprintf("unsupported key type %T", arg))
		}
	}
	var b *bolt.Bucket
	created := false
	for _, key := range keys {
		b = bkt.Bucket(key)
		if b == nil && !create {
			return nil, false
		} else if b == nil {
			created = true
			var err error
			b, err = bkt.CreateBucket(key)
			if err != nil {
				return nil, false
			}
		}
		bkt = b
	}
	return b, created
}

func put(b *bolt.Bucket, k, v interface{}) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return err
	}
	return b.Put(key(k), buf.Bytes())
}

func get(b *bolt.Bucket, k, v interface{}) (bool, error) {
	p := b.Get(key(k))
	if p == nil {
		return false, nil
	}
	err := unmarshal(p, v)
	if err != nil {
		return false, err
	}
	return true, nil
}

func unmarshal(p []byte, ptr interface{}) error {
	return gob.NewDecoder(bytes.NewReader(p)).Decode(ptr)
}

func unmarshalMetrics(b *bolt.Bucket) ([]diviner.Metrics, error) {
	b = lookup(b, metricsKey)
	if b == nil {
		return nil, nil
	}
	var list []diviner.Metrics
	err := b.ForEach(func(k, v []byte) error {
		var metrics diviner.Metrics
		if err := unmarshal(v, &metrics); err != nil {
			return err
		}
		list = append(list, metrics)
		return nil
	})
	return list, err
}

func key(k interface{}) []byte {
	var key []byte
	switch arg := k.(type) {
	case uint64:
		key = make([]byte, 8)
		binary.BigEndian.PutUint64(key, arg)
	case []byte:
		key = arg
	default:
		panic(arg)
	}
	return key
}

func deflate(p []byte) ([]byte, error) {
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	if _, err := w.Write(p); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func inflate(p []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(p))
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(r)
}
