// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package localdb implements a diviner database on the local file
// system using boltdb.
package localdb

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
	"strings"

	"github.com/grailbio/diviner"
	bolt "go.etcd.io/bbolt"
)

var (
	// ErrNoSuchRun is returned when the requested run does not exist.
	ErrNoSuchRun = errors.New("no such run")
	// ErrIncompleteRun is returned when the requested run was not yet
	// completed.
	ErrIncompleteRun = errors.New("incomplete run")
	// ErrInvalidRunId is returned when an invalid run ID was provided.
	ErrInvalidRunId = errors.New("invalid run ID")
	// ErrNoSuchStudy is returned when the requested study does not
	// exist.
	ErrNoSuchStudy = errors.New("no such study")
)

var (
	studiesKey = []byte("studies")
	metaKey    = []byte("meta")
	runsKey    = []byte("runs")
	logsKey    = []byte("logs")
	metricsKey = []byte("metrics")
)

// DB implements diviner.Database using Bolt.
type DB struct{ db *bolt.DB }

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

// New implements diviner.Database.
func (d *DB) New(ctx context.Context, study diviner.Study, values diviner.Values) (diviner.Run, error) {
	run := new(run)
	return run, d.db.Update(func(tx *bolt.Tx) (e error) {
		b := tx.Bucket(studiesKey).Bucket([]byte(study.Name))
		if b == nil {
			var err error
			if b, err = tx.Bucket(studiesKey).CreateBucket([]byte(study.Name)); err != nil {
				return err
			}
			if err := put(b, metaKey, study.Params); err != nil {
				return err
			}
		}
		var err error
		if b, err = b.CreateBucketIfNotExists(runsKey); err != nil {
			return err
		}
		run.Seq, _ = b.NextSequence()
		run.Study = study.Name
		run.Values = values
		run.init(d.db)
		if _, err = b.CreateBucketIfNotExists(run.seq()); err != nil {
			return err
		}
		return run.marshal(tx)
	})
}

// Run implements diviner.Database.
func (d *DB) Run(ctx context.Context, id string) (diviner.Run, error) {
	parts := strings.SplitN(id, "/", 2)
	if len(parts) != 2 || parts[0] == "" {
		return nil, ErrInvalidRunId
	}
	seq, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return nil, ErrInvalidRunId
	}
	run := &run{
		Seq:   seq,
		Study: parts[0],
	}
	run.init(d.db)
	return run, d.db.View(run.unmarshal)
}

// Runs implements diviner.Database.
func (d *DB) Runs(ctx context.Context, study diviner.Study, states diviner.RunState) (runs []diviner.Run, err error) {
	err = d.db.View(func(tx *bolt.Tx) error {
		b := bucket(tx, studiesKey, []byte(study.Name))
		if b == nil {
			return ErrNoSuchStudy
		}
		b = b.Bucket(runsKey)
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			run := &run{
				Seq:   seq(k),
				Study: study.Name,
			}
			run.init(d.db)
			if err := run.unmarshal(tx); err != nil {
				return err
			}
			if state := run.State(); state&states == state {
				runs = append(runs, run)
			}
			return nil
		})
	})
	return
}

// A run represents a single Diviner run. It implements diviner.Run.
type run struct {
	Seq      uint64
	Study    string
	Values   diviner.Values
	RunState diviner.RunState

	db *bolt.DB
	wr *bufio.Writer
}

// Equal compares two *runs for testing purposes.
func (r *run) Equal(u diviner.Run) bool {
	ru := u.(*run)
	return r.Seq == ru.Seq && r.Study == ru.Study && r.Values.Equal(ru.Values) && r.RunState == ru.RunState
}

func (r *run) init(db *bolt.DB) {
	r.db = db
	r.wr = bufio.NewWriterSize(runWriter{r}, 4<<10)
	if r.RunState == 0 {
		r.RunState = diviner.Pending
	}
}

func (r *run) seq() []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, r.Seq)
	return b
}

func (r *run) bucket(tx *bolt.Tx, which []byte) (*bolt.Bucket, error) {
	b := bucket(tx, studiesKey, []byte(r.Study), runsKey, r.seq())
	if b == nil {
		return nil, ErrNoSuchRun
	}
	if which == nil {
		return b, nil
	}
	return b.CreateBucketIfNotExists(which)
}

func (r *run) marshal(tx *bolt.Tx) error {
	b, err := r.bucket(tx, nil)
	if err != nil {
		return err
	}
	return put(b, metaKey, r)
}

func (r *run) unmarshal(tx *bolt.Tx) error {
	b, err := r.bucket(tx, nil)
	if err != nil {
		return err
	}
	ok, err := get(b, metaKey, r)
	if err == nil && !ok {
		err = ErrNoSuchRun
	}
	return err
}

// Write implements diviner.Run.
func (r *run) Write(p []byte) (n int, err error) {
	return r.wr.Write(p)
}

// Flush implements diviner.Run.
func (r *run) Flush() error {
	return r.wr.Flush()
}

// ID returns a unique identifier for this run in its database.
func (r *run) ID() string {
	return fmt.Sprintf("%s/%d", r.Study, r.Seq)
}

// State implemnets diviner.Run.
func (r *run) State() diviner.RunState {
	return r.RunState
}

// Update implements diviner.Run.
func (r *run) Update(ctx context.Context, metrics diviner.Metrics) error {
	return r.db.Update(func(tx *bolt.Tx) error {
		b, err := r.bucket(tx, metricsKey)
		if err != nil {
			return err
		}
		seq, _ := b.NextSequence()
		return put(b, seq, metrics)
	})
}

// Trial implements diviner.Run.
func (r *run) Trial(ctx context.Context) (trial diviner.Trial, err error) {
	trial.Values = r.Values
	err = r.db.View(func(tx *bolt.Tx) error {
		b, err := r.bucket(tx, nil)
		if err != nil {
			return err
		}
		b = b.Bucket(metricsKey)
		if b == nil {
			return ErrIncompleteRun
		}
		seq := b.Sequence()
		ok, err := get(b, seq, &trial.Metrics)
		if err == nil && !ok {
			err = ErrIncompleteRun
		}
		return err
	})
	return
}

// Complete implements diviner.Run.
func (r *run) Complete(ctx context.Context) error {
	return r.db.Update(func(tx *bolt.Tx) error {
		save := r.RunState
		r.RunState = diviner.Complete
		err := r.marshal(tx)
		if err != nil {
			r.RunState = save
		}
		return err
	})
}

// Log implements diviner.Run.
func (r *run) Log() io.Reader {
	return &runReader{run: r, whence: 1}
}

type runWriter struct{ *run }

func (w runWriter) Write(p []byte) (n int, err error) {
	n = len(p)
	err = w.db.Update(func(tx *bolt.Tx) error {
		b, err := w.bucket(tx, logsKey)
		if err != nil {
			return err
		}
		p, err = deflate(p)
		if err != nil {
			return err
		}
		seq, _ := b.NextSequence()
		return b.Put(key(seq), p)
	})
	if err != nil {
		n = 0
	}
	return
}

type runReader struct {
	*run
	whence uint64
	buf    []byte
}

func (r *runReader) Read(p []byte) (n int, err error) {
	for len(r.buf) == 0 {
		err = r.db.View(func(tx *bolt.Tx) error {
			b, err := r.bucket(tx, nil)
			if err != nil {
				return err
			}
			b = b.Bucket(logsKey)
			if b == nil {
				return io.EOF
			}
			r.buf = b.Get(key(r.whence))
			if r.buf == nil {
				return io.EOF
			}
			r.buf, err = inflate(r.buf)
			if err != nil {
				return err
			}
			r.whence++
			return nil
		})
		if err != nil {
			return
		}
	}
	n = copy(p, r.buf)
	r.buf = r.buf[n:]
	return
}

type bucketer interface{ Bucket(key []byte) *bolt.Bucket }

func bucket(bkt bucketer, root []byte, keys ...[]byte) *bolt.Bucket {
	b := bkt.Bucket(root)
	if b == nil {
		return nil
	}
	for _, key := range keys {
		b = b.Bucket(key)
		if b == nil {
			return nil
		}
	}
	return b
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
	err := gob.NewDecoder(bytes.NewReader(p)).Decode(v)
	if err != nil {
		return false, err
	}
	return true, nil
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

func seq(p []byte) uint64 {
	if len(p) != 8 {
		panic(len(p))
	}
	return binary.BigEndian.Uint64(p)
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
