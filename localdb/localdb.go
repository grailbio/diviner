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
	"sync"
	"time"

	"github.com/grailbio/base/log"
	"github.com/grailbio/diviner"
	bolt "go.etcd.io/bbolt"
)

var (
	// ErrNoSuchRun is returned when the requested run does not exist.
	ErrNoSuchRun = errors.New("no such run")
	// ErrInvalidRunId is returned when an invalid run ID was provided.
	ErrInvalidRunId = errors.New("invalid run ID")
	// ErrNoSuchStudy is returned when the requested study does not
	// exist.
	ErrNoSuchStudy = errors.New("no such study")
)

var (
	studiesKey = []byte("studies")
	metaKey    = []byte("meta")
	updatedKey = []byte("updated")
	runsKey    = []byte("runs")
	logsKey    = []byte("logs")
	metricsKey = []byte("metrics")
)

// RunKey uniquely identifies a run as stored by the DB.
type runKey struct {
	study string
	seq   uint64
}

// DB implements diviner.Database using Bolt.
type DB struct {
	db *bolt.DB

	mu sync.Mutex
	// Live is the set of live runs.
	live map[runKey]bool
}

// Open opens and returns a new database with the provided filename.
// The file is created if it does not already exist.
func Open(filename string) (db *DB, err error) {
	db = &DB{live: make(map[runKey]bool)}
	db.db, err = bolt.Open(filename, 0666, nil)
	if err != nil {
		return nil, err
	}
	return db, db.db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucketIfNotExists(studiesKey)
		return err
	})
}

// Study implements diviner.Database.
func (d *DB) Study(ctx context.Context, name string) (study diviner.Study, err error) {
	err = d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(studiesKey)
		if b == nil {
			return ErrNoSuchStudy
		}
		b = b.Bucket([]byte(name))
		if b == nil {
			return ErrNoSuchStudy
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
func (d *DB) Studies(ctx context.Context, prefix string, since time.Time) (studies []diviner.Study, err error) {
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

// New implements diviner.Database.
func (d *DB) New(ctx context.Context, study diviner.Study, values diviner.Values, config diviner.RunConfig) (diviner.Run, error) {
	run := new(run)
	err := d.db.Update(func(tx *bolt.Tx) (e error) {
		b := tx.Bucket(studiesKey).Bucket([]byte(study.Name))
		if b == nil {
			var err error
			if b, err = tx.Bucket(studiesKey).CreateBucket([]byte(study.Name)); err != nil {
				return err
			}
			if err := put(b, updatedKey, time.Now()); err != nil {
				return err
			}
			if err := put(b, metaKey, study); err != nil {
				return err
			}
		}
		var err error
		if b, err = b.CreateBucketIfNotExists(runsKey); err != nil {
			return err
		}
		run.Seq, _ = b.NextSequence()
		run.Study = study.Name
		run.RunValues = values
		// TODO(marius): include the rest of the run config as well.
		// We should also clearly delineate in diviner's data structures
		// which parts are dynamic/serializable and which contain state
		// that cannot be serialized.
		run.RunConfig = config
		run.RunCreated = time.Now()
		run.init(d.db)
		if _, err = b.CreateBucketIfNotExists(run.seq()); err != nil {
			return err
		}
		return run.marshal(tx)
	})
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	d.live[runKey{run.Study, run.Seq}] = true
	d.mu.Unlock()
	return run, nil
}

// Run implements diviner.Database.
func (d *DB) Run(ctx context.Context, study, id string) (diviner.Run, error) {
	seq, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		return nil, ErrInvalidRunId
	}
	run := &run{
		Seq:   seq,
		Study: study,
	}
	run.init(d.db)
	return run, d.db.View(run.unmarshal)
}

// Runs implements diviner.Database.
func (d *DB) Runs(ctx context.Context, study string, states diviner.RunState, since time.Time) (runs []diviner.Run, err error) {
	err = d.db.View(func(tx *bolt.Tx) error {
		b := bucket(tx, studiesKey, []byte(study))
		if b == nil {
			return ErrNoSuchStudy
		}
		b = b.Bucket(runsKey)
		if b == nil {
			return nil
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		return b.ForEach(func(k, v []byte) error {
			run := &run{
				Seq:   seq(k),
				Study: study,
			}
			run.init(d.db)
			if err := run.unmarshal(tx); err != nil {
				return err
			}
			if !since.IsZero() {
				updated, err := run.updated(tx)
				if err != nil {
					return err
				}
				if updated.Before(since) {
					return nil
				}
			}
			// If we are querying for pending runs, they must be in the liveset;
			// otherwise they are orphaned.
			if state := run.State(); state&states == state && (state != diviner.Pending || d.live[runKey{run.Study, run.Seq}]) {
				runs = append(runs, run)
			}
			return nil
		})
	})
	return
}

// A run represents a single Diviner run. It implements diviner.Run.
type run struct {
	Seq        uint64
	Study      string
	RunValues  diviner.Values
	RunState   diviner.RunState
	RunConfig  diviner.RunConfig
	RunCreated time.Time
	RunRuntime time.Duration

	db *bolt.DB
	wr *bufio.Writer

	mu     sync.Mutex
	status string
}

// Equal compares two *runs for testing purposes.
func (r *run) Equal(u diviner.Run) bool {
	ru := u.(*run)
	return r.Seq == ru.Seq && r.Study == ru.Study && r.RunValues.Equal(ru.RunValues) && r.RunState == ru.RunState
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
	err = put(b, metaKey, r)
	if err := put(bucket(tx, studiesKey, []byte(r.Study)), updatedKey, time.Now()); err != nil {
		log.Error.Printf("run %v: update: %s", r, err)
	}
	return err
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

func (r *run) updated(tx *bolt.Tx) (time.Time, error) {
	b, err := r.bucket(tx, nil)
	if err != nil {
		return time.Time{}, err
	}
	var t time.Time
	ok, err := get(b, updatedKey, &t)
	if err == nil && !ok {
		err = ErrNoSuchRun
	}
	return t, err
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
	return fmt.Sprint(r.Seq)
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

// SetStatus implements diviner.Run.
func (r *run) SetStatus(ctx context.Context, status string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.status = status
	return nil
}

// Status implements diviner.Run.
func (r *run) Status(ctx context.Context) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.status, nil
}

// Created implements diviner.Run.
func (r *run) Created() time.Time { return r.RunCreated }

// Runtime implements diviner.Run.
func (r *run) Runtime() time.Duration { return r.RunRuntime }

// Config implements diviner.Run.
func (r *run) Config() diviner.RunConfig { return r.RunConfig }

// Values implements diviner.Run.
func (r *run) Values() diviner.Values {
	return r.RunValues
}

// Metrics implements diviner.Run.
func (r *run) Metrics(ctx context.Context) (metrics diviner.Metrics, err error) {
	err = r.db.View(func(tx *bolt.Tx) error {
		b, err := r.bucket(tx, nil)
		if err != nil {
			return err
		}
		b = b.Bucket(metricsKey)
		if b == nil {
			return nil
		}
		seq := b.Sequence()
		_, err = get(b, seq, &metrics)
		return err
	})
	return
}

// Complete implements diviner.Run.
func (r *run) Complete(ctx context.Context, state diviner.RunState, runtime time.Duration) error {
	return r.db.Update(func(tx *bolt.Tx) error {
		var (
			saveState   = r.RunState
			saveRuntime = r.RunRuntime
		)
		r.RunState = state
		r.RunRuntime = runtime
		err := r.marshal(tx)
		if err != nil {
			r.RunState = saveState
			r.RunRuntime = saveRuntime
		}
		return err
	})
}

// Log implements diviner.Run.
func (r *run) Log(follow bool) io.Reader {
	return &runReader{run: r, whence: 1, follow: follow}
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

var errEndOfStream = errors.New("end of stream")

type runReader struct {
	*run
	whence uint64
	buf    []byte
	follow bool
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
				return errEndOfStream
			}
			r.buf, err = inflate(r.buf)
			if err != nil {
				return err
			}
			r.whence++
			return nil
		})
		if err == errEndOfStream {
			if r.follow {
				// We could do better here since writes are happening
				// in the same process. But this is simpler and it works.
				time.Sleep(5 * time.Second)
				continue
			}
			err = io.EOF
		}
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
