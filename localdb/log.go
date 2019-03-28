// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package localdb

import (
	"bufio"
	"errors"
	"io"
	"time"

	bolt "go.etcd.io/bbolt"
)

var errEndOfStream = errors.New("end of stream")

type writeFlusher interface {
	io.Writer
	Flush() error
}

type flushCloser struct {
	writeFlusher
}

func (f flushCloser) Close() error {
	return f.Flush()
}

type runWriter struct {
	db    *bolt.DB
	study string
	seq   uint64
}

func (d *DB) Logger(study string, seq uint64) io.WriteCloser {
	return flushCloser{bufio.NewWriterSize(runWriter{d.db, study, seq}, 4<<10)}
}

func (w runWriter) Write(p []byte) (n int, err error) {
	n = len(p)
	err = w.db.Update(func(tx *bolt.Tx) error {
		b, _ := create(tx, runKey{w.study, w.seq}, logsKey)
		if b == nil {
			return errors.New("failed to create logs bucket")
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

func (runWriter) Flush() error { return nil }
func (runWriter) Close() error { return nil }

type runReader struct {
	db     *bolt.DB
	study  string
	seq    uint64
	whence uint64
	buf    []byte
	follow bool
}

// Log implements diviner.Run.
func (d *DB) Log(study string, seq uint64, follow bool) io.Reader {
	return &runReader{db: d.db, study: study, seq: seq, whence: 1, follow: follow}
}

func (r *runReader) Read(p []byte) (n int, err error) {
	for len(r.buf) == 0 {
		err = r.db.View(func(tx *bolt.Tx) error {
			b := lookup(tx, runKey{r.study, r.seq})
			if b == nil {
				return ErrNoSuchRun
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
