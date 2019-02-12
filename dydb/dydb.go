// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package dydb implements a diviner.Database on top of dynamodb
// and the AWS cloudwatch logs storage. Database instances may be
// safely shared between multiple users.
package dydb

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/sync/once"
	"github.com/grailbio/diviner"
)

// ErrIncompleteTrial indicates that the trial has not yet completed.
var ErrIncompleteTrial = errors.New("incomplete trial")

var logGroups once.Map

// A DB represents a session to a DynamoDB table; it implements
// diviner.Database.
type DB struct {
	sess  *session.Session
	db    *dynamodb.DynamoDB
	table string
}

// New creates a new DB instance from the provided session and table name.
func New(sess *session.Session, table string) *DB {
	return &DB{
		sess:  sess,
		db:    dynamodb.New(sess),
		table: table,
	}
}

// New implements diviner.Database.
func (d *DB) New(ctx context.Context, study diviner.Study, values diviner.Values) (diviner.Run, error) {
	var b bytes.Buffer
	if err := gob.NewEncoder(&b).Encode(values); err != nil {
		return nil, err
	}
	seq, err := d.nextSeq(ctx, study)
	if err != nil {
		return nil, err
	}
	_, err = d.db.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(d.table),
		Item: map[string]*dynamodb.AttributeValue{
			"study": {S: aws.String(study.Name)},
			"run":   {N: aws.String(fmt.Sprint(seq))},
			// TODO(marius): it might be nice to expose these to dynamodb
			// so they can be part of direct queries.
			"values":       {B: b.Bytes()},
			"metrics_list": {L: []*dynamodb.AttributeValue{}},
			"complete":     {BOOL: aws.Bool(false)},
		},
	})
	if err != nil {
		return nil, err
	}
	return &run{
		sess:      d.sess,
		db:        d.db,
		table:     d.table,
		Values:    values,
		studyName: study.Name,
		seq:       seq,
		state:     diviner.Pending,
	}, nil
}

// Runs implements diviner.Database.
func (d *DB) Runs(ctx context.Context, study diviner.Study, states diviner.RunState) (runs []diviner.Run, err error) {
	var lastKey map[string]*dynamodb.AttributeValue
	for {
		input := &dynamodb.ScanInput{
			TableName:        aws.String(d.table),
			FilterExpression: aws.String(`study = :study AND run > :zero`),
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":study": {S: aws.String(study.Name)},
				":zero":  {N: aws.String("0")},
			},
		}
		if lastKey != nil {
			input.ExclusiveStartKey = lastKey
		}
		out, err := d.db.ScanWithContext(ctx, input)
		if err != nil {
			return nil, err
		}
		for _, item := range out.Items {
			r := &run{sess: d.sess, db: d.db, table: d.table}
			if err := r.unmarshal(item); err != nil {
				return nil, err
			}
			if r.State()&states == r.State() {
				runs = append(runs, r)
			}
		}
		lastKey = out.LastEvaluatedKey
		if lastKey == nil {
			break
		}
	}
	return
}

// Run implements diviner.Database.
func (d *DB) Run(ctx context.Context, id string) (diviner.Run, error) {
	parts := strings.SplitN(id, "/", 3)
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid run key %q", id)
	}
	table, study := parts[0], parts[1]
	seq, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid run key %q: %v", id, err)
	}
	r := &run{sess: d.sess, db: d.db, table: table, seq: seq, studyName: study}
	if err := r.get(); err != nil {
		return nil, err
	}
	return r, nil
}

// nextSeq retrieves the next run ID for the provided study. A meta
// entry for the study is created if it does not yet exist.
func (d *DB) nextSeq(ctx context.Context, study diviner.Study) (uint64, error) {
	var b bytes.Buffer
	if err := gob.NewEncoder(&b).Encode(study); err != nil {
		return 0, err
	}
	_, err := d.db.PutItemWithContext(ctx, &dynamodb.PutItemInput{
		TableName:           aws.String(d.table),
		ConditionExpression: aws.String(`attribute_not_exists(study)`),
		Item: map[string]*dynamodb.AttributeValue{
			"study":       {S: aws.String(study.Name)},
			"run":         {N: aws.String("0")},
			"num_studies": {N: aws.String("0")},
			"meta":        {B: b.Bytes()},
		},
	})
	if err != nil {
		aerr, ok := err.(awserr.Error)
		if !ok || aerr.Code() != "ConditionalCheckFailedException" {
			return 0, err
		}
	}
	out, err := d.db.UpdateItemWithContext(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(d.table),
		Key: map[string]*dynamodb.AttributeValue{
			"study": {S: aws.String(study.Name)},
			"run":   {N: aws.String("0")},
		},
		UpdateExpression: aws.String(`SET num_studies = num_studies + :one`),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":one": {N: aws.String("1")},
		},
		ReturnValues: aws.String(`UPDATED_NEW`),
	})
	if err != nil {
		return 0, err
	}
	val, ok := out.Attributes["num_studies"]
	if !ok || val.N == nil {
		return 0, errors.New("dynamodb did not return count")
	}
	return strconv.ParseUint(*val.N, 10, 64)
}

// A run is a single diviner run. It implements diviner.Run on top of dynamodb.
type run struct {
	sess  *session.Session
	db    *dynamodb.DynamoDB
	table string
	diviner.Values
	studyName string
	seq       uint64
	state     diviner.RunState

	once   sync.Once
	writec chan []byte
	flushc chan chan error

	cloudwatchOnce once.Task
	logs           *cloudwatchlogs.CloudWatchLogs
	logsSeq        *string
}

func (r *run) get() error {
	out, err := r.db.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(r.table),
		Key:       r.key(),
	})
	if err != nil {
		return err
	}
	return r.unmarshal(out.Item)
}

func (r *run) unmarshal(attrs map[string]*dynamodb.AttributeValue) error {
	if study := attrs["study"]; study == nil || study.S == nil {
		return errors.New("missing study name")
	}
	if run := attrs["run"]; run == nil || run.N == nil {
		return errors.New("missing run")
	}
	if values := attrs["values"]; values == nil || values.B == nil {
		return errors.New("missing values")
	}
	if complete := attrs["complete"]; complete == nil || complete.BOOL == nil {
		return errors.New("missing complete")
	}
	r.studyName = *attrs["study"].S
	var err error
	r.seq, err = strconv.ParseUint(*attrs["run"].N, 10, 64)
	if err != nil {
		return err
	}
	if err := gob.NewDecoder(bytes.NewReader(attrs["values"].B)).Decode(&r.Values); err != nil {
		return err
	}
	if *attrs["complete"].BOOL {
		r.state = diviner.Complete
	} else {
		r.state = diviner.Pending
	}
	return nil
}

// Write implements diviner.Run.
func (r *run) Write(p []byte) (n int, err error) {
	r.flusher(false)
	p1 := make([]byte, len(p))
	copy(p1, p)
	r.writec <- p1
	return len(p), nil
}

// Flush implements diviner.Run.
func (r *run) Flush() error {
	r.flusher(false)
	errc := make(chan error)
	r.flushc <- errc
	return <-errc
}

// ID implements diviner.Run.
func (r *run) ID() string {
	return fmt.Sprintf("%s/%s/%d", r.table, r.studyName, r.seq)
}

// State implements diviner.Run.
func (r *run) State() diviner.RunState {
	return r.state
}

// Update implements diviner.Run.
func (r *run) Update(ctx context.Context, metrics diviner.Metrics) error {
	_, err := r.db.UpdateItemWithContext(ctx, &dynamodb.UpdateItemInput{
		TableName:        aws.String(r.table),
		Key:              r.key(),
		UpdateExpression: aws.String(`SET metrics_list = list_append(metrics_list, :metrics)`),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":metrics": {L: []*dynamodb.AttributeValue{metricsValue(metrics)}},
		},
	})
	return err
}

// Trial implements diviner.Run.
func (r *run) Trial(ctx context.Context) (trial diviner.Trial, err error) {
	out, err := r.db.GetItemWithContext(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(r.table),
		Key:       r.key(),
	})
	if err != nil {
		return diviner.Trial{}, err
	}
	list := out.Item["metrics_list"].L
	if len(list) == 0 {
		return diviner.Trial{}, ErrIncompleteTrial
	}
	metrics, err := valueMetrics(list[len(list)-1])
	if err != nil {
		return diviner.Trial{}, err
	}
	return diviner.Trial{r.Values, metrics}, nil
}

// Complete implements diviner.Run.
func (r *run) Complete(ctx context.Context) error {
	_, err := r.db.UpdateItemWithContext(ctx, &dynamodb.UpdateItemInput{
		TableName:        aws.String(r.table),
		Key:              r.key(),
		UpdateExpression: aws.String(`SET complete = :true`),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":true": {BOOL: aws.Bool(true)},
		},
	})
	if err == nil {
		r.state = diviner.Complete
	}
	r.flusher(true)
	return err
}

// Log implements diviner.Run.
func (r *run) Log() io.Reader {
	group, stream := r.streamKeys()
	return &logReader{sess: r.sess, group: group, stream: stream}
}

func (r *run) flusher(stop bool) {
	r.once.Do(func() {
		// Buffer at most 100 writes. Should we drop writes so we never block?
		r.writec = make(chan []byte, 100)
		r.flushc = make(chan chan error)
		go r.flushLoop()
	})
	if stop {
		close(r.writec)
	}
}

func (r *run) flushLoop() {
	var (
		events    []*cloudwatchlogs.InputLogEvent
		buf       bytes.Buffer
		lastFlush = time.Now()
		flush     chan error
		stop      bool
		timer     = time.NewTimer(15 * time.Second)
	)
	defer timer.Stop()
	for {
		if len(events) > 100 || time.Since(lastFlush) > 30*time.Second || flush != nil {
			var err error
			if len(events) > 0 {
				err = r.flush(events)
			}
			lastFlush = time.Now()
			if flush != nil {
				flush <- err
				flush = nil
			}
		}
		if stop {
			return
		}
		select {
		case p, ok := <-r.writec:
			if !ok {
				stop = true
				flush = make(chan error, 1)
				break
			}
			buf.Write(p)
			for {
				n := bytes.Index(buf.Bytes(), []byte{'\n'})
				if n < 0 {
					break
				}
				if n == 0 {
					continue
				}
				line, err := buf.ReadString('\n')
				if err != nil {
					panic(err)
				}
				events = append(events, &cloudwatchlogs.InputLogEvent{
					Timestamp: aws.Int64(time.Now().UnixNano() / 1000000),
					Message:   aws.String(line[:len(line)-1]),
				})
			}
		case flush = <-r.flushc:
		case <-timer.C:
		}
	}
}

func (r *run) flush(events []*cloudwatchlogs.InputLogEvent) error {
	var (
		group, stream = r.streamKeys()
		client        = cloudwatchlogs.New(r.sess)
	)
	err := logGroups.Do(group, func() error {
		_, err := client.CreateLogGroup(&cloudwatchlogs.CreateLogGroupInput{
			LogGroupName: aws.String(group),
		})
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
	err = r.cloudwatchOnce.Do(func() error {
		_, err = client.CreateLogStream(&cloudwatchlogs.CreateLogStreamInput{
			LogGroupName:  aws.String(group),
			LogStreamName: aws.String(stream),
		})
		if err != nil {
			aerr, ok := err.(awserr.Error)
			if ok && aerr.Code() != cloudwatchlogs.ErrCodeResourceAlreadyExistsException {
				log.Error.Printf("failed to create cloudwatch stream: %v", err)
				return err
			}
		}
		r.logs = client
		return nil
	})
	if err != nil {
		return err
	}
	out, err := r.logs.PutLogEvents(&cloudwatchlogs.PutLogEventsInput{
		LogEvents:     events,
		LogGroupName:  aws.String(group),
		LogStreamName: aws.String(stream),
		SequenceToken: r.logsSeq,
	})
	if err != nil {
		log.Error.Printf("CloudWatchLogs.PutLogEvent: %v", err)
	} else {
		r.logsSeq = out.NextSequenceToken
	}
	return err
}

func (r *run) streamKeys() (group, stream string) {
	return fmt.Sprintf("%s/%s", r.table, r.studyName), fmt.Sprint(r.seq)
}

func (r *run) key() map[string]*dynamodb.AttributeValue {
	return map[string]*dynamodb.AttributeValue{
		"study": {S: aws.String(r.studyName)},
		"run":   {N: aws.String(fmt.Sprint(r.seq))},
	}
}

func metricsValue(m diviner.Metrics) *dynamodb.AttributeValue {
	v := new(dynamodb.AttributeValue)
	v.M = make(map[string]*dynamodb.AttributeValue)
	for k, n := range m {
		v.M[k] = &dynamodb.AttributeValue{N: aws.String(fmt.Sprint(n))}
	}
	return v
}

func valueMetrics(v *dynamodb.AttributeValue) (diviner.Metrics, error) {
	metrics := make(diviner.Metrics)
	for k, n := range v.M {
		var err error
		metrics[k], err = strconv.ParseFloat(aws.StringValue(n.N), 64)
		if err != nil {
			return nil, err
		}
	}
	return metrics, nil
}

type logReader struct {
	sess          *session.Session
	group, stream string

	save, buf []byte
	nextToken *string
}

func (r *logReader) Read(p []byte) (n int, err error) {
	for len(r.buf) == 0 {
		var err error
		r.buf, err = r.append(r.save[:0])
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
	out, err := client.GetLogEvents(&cloudwatchlogs.GetLogEventsInput{
		LogGroupName:  aws.String(r.group),
		LogStreamName: aws.String(r.stream),
		StartFromHead: aws.Bool(true),
		NextToken:     r.nextToken,
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == "ResourceNotFoundException" {
			err = io.EOF
		}
		return buf, err
	}
	if len(out.Events) == 0 {
		return buf, io.EOF
	}
	if aws.StringValue(r.nextToken) == aws.StringValue(out.NextForwardToken) {
		return buf, io.EOF
	}
	for _, event := range out.Events {
		m := aws.StringValue(event.Message)
		buf = append(buf, []byte(m)...)
		buf = append(buf, '\n')
	}
	r.nextToken = out.NextForwardToken
	return buf, nil
}
