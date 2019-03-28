// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package dydb

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/sync/once"
)

// Initialization for each unique log group.
var logGroups once.Map

var errEmptyReply = errors.New("empty reply")

type flush struct {
	Errc  chan error
	Close bool
}

type logger struct {
	sess   *session.Session
	group  string
	stream string
	writec chan []byte
	flushc chan flush

	logs     *cloudwatchlogs.CloudWatchLogs
	logsInit once.Task
	logsSeq  *string
}

// Logger returns a logger that writes log messages, line-for-line
// to the AWS CloudWatch Logs service.
func (d *DB) Logger(study string, seq uint64) io.WriteCloser {
	group, stream := d.streamKeys(study, seq)
	l := &logger{
		sess:   d.sess,
		group:  group,
		stream: stream,
		writec: make(chan []byte, 100),
		flushc: make(chan flush),
	}
	go l.Do()
	return l
}

func (l *logger) Write(p []byte) (n int, err error) {
	p1 := make([]byte, len(p))
	copy(p1, p)
	l.writec <- p1
	return len(p), nil
}

func (l *logger) Flush() error {
	errc := make(chan error)
	l.flushc <- flush{Errc: errc}
	return <-errc
}

func (l *logger) Close() error {
	errc := make(chan error)
	l.flushc <- flush{Errc: errc, Close: true}
	return <-errc
}

func (l *logger) Do() {
	var (
		events    []*cloudwatchlogs.InputLogEvent
		buf       bytes.Buffer
		lastFlush = time.Now()
		flush     flush
		timer     = time.NewTimer(15 * time.Second)
	)
	defer timer.Stop()
	for {
		if len(events) > 100 || time.Since(lastFlush) > 30*time.Second || flush.Errc != nil {
			var err error
			if len(events) > 0 {
				err = l.flush(events)
			}
			if err == nil {
				events = nil
			}
			lastFlush = time.Now()
			if flush.Errc != nil {
				flush.Errc <- err
				flush.Errc = nil
			}
		}
		if flush.Close {
			return
		}
		select {
		case p := <-l.writec:
			buf.Write(p)
			for {
				n := bytes.Index(buf.Bytes(), []byte{'\n'})
				if n < 0 {
					break
				}
				line, err := buf.ReadString('\n')
				if err != nil {
					panic(err)
				}
				if strings.TrimSpace(line) == "" {
					continue
				}
				events = append(events, &cloudwatchlogs.InputLogEvent{
					Timestamp: aws.Int64(time.Now().UnixNano() / 1000000),
					Message:   aws.String(line[:len(line)-1]),
				})
			}
		case flush = <-l.flushc:
		case <-timer.C:
		}
	}
}

func (l *logger) flush(events []*cloudwatchlogs.InputLogEvent) error {
	client := cloudwatchlogs.New(l.sess)
	err := logGroups.Do(l.group, func() error {
		input := &cloudwatchlogs.CreateLogGroupInput{
			LogGroupName: aws.String(l.group),
		}
		_, err := client.CreateLogGroup(input)
		debug("cloudwatchlogs.CreateLogGroup", input, nil, err)
		if err != nil {
			aerr, ok := err.(awserr.Error)
			if ok && aerr.Code() == cloudwatchlogs.ErrCodeResourceAlreadyExistsException {
				err = nil
			}
		}
		return err
	})
	if err != nil {
		return err
	}
	err = l.logsInit.Do(func() error {
		input := &cloudwatchlogs.CreateLogStreamInput{
			LogGroupName:  aws.String(l.group),
			LogStreamName: aws.String(l.stream),
		}
		_, err = client.CreateLogStream(input)
		debug("cloudwatchlogs.CreateLogStream", input, nil, err)
		if err != nil {
			aerr, ok := err.(awserr.Error)
			if ok && aerr.Code() != cloudwatchlogs.ErrCodeResourceAlreadyExistsException {
				log.Error.Printf("failed to create cloudwatch stream: %v", err)
				return err
			}
		}
		log.Printf("dydb: created cloudwatch stream, group %s, name %s", l.group, l.stream)
		l.logs = client
		return nil
	})
	if err != nil {
		return err
	}
	input := &cloudwatchlogs.PutLogEventsInput{
		LogEvents:     events,
		LogGroupName:  aws.String(l.group),
		LogStreamName: aws.String(l.stream),
		SequenceToken: l.logsSeq,
	}
	out, err := l.logs.PutLogEvents(input)
	debug("cloudwatchlogs.PutLogEvents", input, out, err)
	if err != nil {
		var seq string
		if l.logsSeq != nil {
			seq = *l.logsSeq
		}
		log.Error.Printf("CloudWatchLogs.PutLogEvent(seq: %v): %v", seq, err)
		// Clear the sequence, in case the error is due to missynchronized sequence
		// tokens.  This could happens when two diviner instances are writing to the
		// same stream due to external race.
		l.logsSeq = nil
	} else {
		l.logsSeq = out.NextSequenceToken
	}
	return err
}

type logReader struct {
	sess          *session.Session
	group, stream string

	follow    bool
	save, buf []byte
	nextToken *string
}

// Log returns an io.Reader that reads log messages from
// the AWS CloudWatch Logs service.
func (d *DB) Log(study string, seq uint64, follow bool) io.Reader {
	group, stream := d.streamKeys(study, seq)
	return &logReader{sess: d.sess, group: group, stream: stream, follow: follow}
}

func (r *logReader) Read(p []byte) (n int, err error) {
	for len(r.buf) == 0 {
		var err error
		r.buf, err = r.append(r.save[:0])
		if err == errEmptyReply {
			if r.follow {
				time.Sleep(5 * time.Second)
				continue
			}
			err = io.EOF
		}
		if err != nil {
			return 0, err
		}
		r.save = r.buf
	}
	n = copy(p, r.buf)
	r.buf = r.buf[n:]
	return
}

func (r *logReader) append(buf []byte) ([]byte, error) {
	client := cloudwatchlogs.New(r.sess)
	input := &cloudwatchlogs.GetLogEventsInput{
		LogGroupName:  aws.String(r.group),
		LogStreamName: aws.String(r.stream),
		StartFromHead: aws.Bool(true),
		NextToken:     r.nextToken,
	}
	out, err := client.GetLogEvents(input)
	debug("cloudwatchlogs.GetLogEvents", input, out, err)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == "ResourceNotFoundException" {
			err = io.EOF
		}
		return buf, err
	}
	if len(out.Events) == 0 {
		return buf, errEmptyReply
	}
	if aws.StringValue(r.nextToken) == aws.StringValue(out.NextForwardToken) {
		return buf, errEmptyReply
	}
	for _, event := range out.Events {
		m := aws.StringValue(event.Message)
		buf = append(buf, []byte(m)...)
		buf = append(buf, '\n')
	}
	r.nextToken = out.NextForwardToken
	return buf, nil
}

func (d *DB) streamKeys(study string, seq uint64) (group, stream string) {
	study = strings.Replace(study, ",", "/", -1)
	study = strings.Replace(study, "=", "_", -1)
	return fmt.Sprintf("%s/%s", d.table, study), fmt.Sprint(seq)
}
