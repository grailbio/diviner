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
	"math"
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
	"github.com/grailbio/base/traverse"
	"github.com/grailbio/diviner"
	"golang.org/x/time/rate"
)

// TODO(marius): get rid of using gob here; either encode data directly in dynamoDB
// attributes or use JSON.

const (
	keepaliveInterval = 30 * time.Second
	// The time layout used to store timestamps in dynamodb.
	// RFC3339 timestamps order lexically.
	timeLayout = time.RFC3339

	// DateLayout is used to partition the keepalive index.
	dateLayout = "2006-01-02"

	// ScanSegments is the number of concurrent scan operations we perform.
	scanSegments = 50

	keepaliveIndexName = "date-keepalive-index"
)

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

// Study implements diviner.Database.
func (d *DB) Study(ctx context.Context, name string) (study diviner.Study, err error) {
	log.Debug.Printf("dydb.Study: %s", name)
	out, err := d.db.GetItemWithContext(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(d.table),
		Key: map[string]*dynamodb.AttributeValue{
			"study": {S: aws.String(name)},
			"run":   {N: aws.String("0")},
		},
	})
	if err != nil {
		return
	}
	err = gob.NewDecoder(bytes.NewReader(out.Item["meta"].B)).Decode(&study)
	return
}

// Studies implements diviner.Database.
func (d *DB) Studies(ctx context.Context, prefix string, since time.Time) ([]diviner.Study, error) {
	log.Debug.Printf("dydb.Studies: %s %s", prefix, since)
	if !since.IsZero() {
		items, err := d.querySince(ctx, since, func() *dynamodb.QueryInput {
			query := &dynamodb.QueryInput{
				FilterExpression:         aws.String(`attribute_exists(#meta)`),
				ExpressionAttributeNames: appendAttributeNames(nil, "meta"),
			}
			if prefix != "" {
				query.FilterExpression = aws.String(*query.FilterExpression + ` AND begins_with(#study, :prefix)`)
				query.ExpressionAttributeValues = map[string]*dynamodb.AttributeValue{
					":prefix": {S: aws.String(prefix)},
				}
				query.ExpressionAttributeNames = appendAttributeNames(query.ExpressionAttributeNames, "study")
			}
			return query
		})
		if err != nil {
			return nil, err
		}
		return appendStudies(nil, items...), nil
	}
	segments := make([][]diviner.Study, scanSegments)
	err := traverse.Each(len(segments), func(i int) (err error) {
		segments[i], err = d.studies(ctx, prefix, i, len(segments))
		return
	})
	if err != nil {
		return nil, err
	}
	var studies []diviner.Study
	for _, segment := range segments {
		studies = append(studies, segment...)
	}
	return studies, nil
}

func (d *DB) studies(ctx context.Context, prefix string, segment, totalSegments int) ([]diviner.Study, error) {
	input := &dynamodb.ScanInput{
		TableName:                aws.String(d.table),
		Segment:                  aws.Int64(int64(segment)),
		TotalSegments:            aws.Int64(int64(totalSegments)),
		FilterExpression:         aws.String(`attribute_exists(#meta)`),
		ExpressionAttributeNames: appendAttributeNames(nil, "meta"),
	}
	if prefix != "" {
		input.FilterExpression = aws.String(*input.FilterExpression + ` AND begins_with(#study, :prefix)`)
		input.ExpressionAttributeValues = map[string]*dynamodb.AttributeValue{
			":prefix": {S: aws.String(prefix)},
		}
		input.ExpressionAttributeNames = appendAttributeNames(input.ExpressionAttributeNames, "study")
	}
	var (
		studies []diviner.Study
		lastKey map[string]*dynamodb.AttributeValue
	)
	for {
		filters := []string{`attribute_exists(#meta)`}
		if prefix != "" {
			filters = append(filters, ``)
		}
		if lastKey != nil {
			input.ExclusiveStartKey = lastKey
		}
		log.Debug.Printf("dynamodb: query: %s", input)
		out, err := d.db.ScanWithContext(ctx, input)
		if err != nil {
			return nil, err
		}
		studies = appendStudies(studies, out.Items...)
		lastKey = out.LastEvaluatedKey
		if lastKey == nil {
			break
		}
	}
	return studies, nil
}

// New implements diviner.Database.
func (d *DB) New(ctx context.Context, study diviner.Study, values diviner.Values, config diviner.RunConfig) (diviner.Run, error) {
	var b bytes.Buffer
	if err := gob.NewEncoder(&b).Encode(values); err != nil {
		return nil, err
	}
	seq, err := d.nextSeq(ctx, study)
	if err != nil {
		return nil, err
	}
	var configBuf bytes.Buffer
	if err := gob.NewEncoder(&configBuf).Encode(config); err != nil {
		return nil, err
	}
	// TODO(marius): verify that studies are compatible: that both the
	// names and actual study metadata matches.
	var (
		now       = time.Now().UTC()
		startTime = now.Format(timeLayout)
		startDate = now.Format(dateLayout)
	)
	_, err = d.db.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(d.table),
		Item: map[string]*dynamodb.AttributeValue{
			"study": {S: aws.String(study.Name)},
			"run":   {N: aws.String(fmt.Sprint(seq))},
			// TODO(marius): it might be nice to expose these to dynamodb
			// so they can be part of direct queries.
			"values":    {B: b.Bytes()},
			"metrics":   {L: []*dynamodb.AttributeValue{}},
			"state":     {S: aws.String(diviner.Pending.String())},
			"timestamp": {S: aws.String(startTime)},
			"date":      {S: aws.String(startDate)},
			"keepalive": {S: aws.String(startTime)},
			// TODO(marius): include a "frozen" config (e.g., where the files
			// include checksums, etc.), so that we can re-create the config
			// independently of local disk state.
			"config": {B: configBuf.Bytes()},
		},
	})
	if err != nil {
		return nil, err
	}
	// Also update the study
	_, err = d.db.UpdateItemWithContext(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(d.table),
		Key: map[string]*dynamodb.AttributeValue{
			"study": {S: aws.String(study.Name)},
			"run":   {N: aws.String("0")},
		},
		UpdateExpression: aws.String(`SET #keepalive = :timestamp, #date = :date`),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":timestamp": {S: aws.String(startTime)},
			":date":      {S: aws.String(startDate)},
		},
		ExpressionAttributeNames: appendAttributeNames(nil, "keepalive", "date"),
	})
	if err != nil {
		log.Error.Printf("failed to update keepalive for study %s: %v", study.Name, err)
	}
	bgctx, cancel := context.WithCancel(context.Background())
	r := &run{
		sess:      d.sess,
		db:        d.db,
		table:     d.table,
		values:    values,
		studyName: study.Name,
		seq:       seq,
		state:     diviner.Pending,
		config:    config,
		created:   now,
		updated:   now,
		cancel:    cancel,
		statusc:   make(chan string, 1),
	}
	go r.keepalive(bgctx)
	go r.updater(bgctx)
	return r, nil
}

// Runs implements diviner.Database.
func (d *DB) Runs(ctx context.Context, study string, states diviner.RunState, since time.Time) (runs []diviner.Run, err error) {
	log.Debug.Printf("dydb.Runs: %s %s %s", study, since, states)
	minPendingTime := time.Now().Add(-2 * keepaliveInterval)
	if since.IsZero() && states == diviner.Pending {
		since = minPendingTime
	}
	if !since.IsZero() {
		items, err := d.querySince(ctx, since, func() *dynamodb.QueryInput {
			return &dynamodb.QueryInput{
				FilterExpression: aws.String(`#study = :study AND #run > :zero`),
				ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
					":study": {S: aws.String(study)},
					":zero":  {N: aws.String("0")},
				},
				ExpressionAttributeNames: appendAttributeNames(nil, "study", "run"),
			}
		})
		if err != nil {
			return nil, err
		}
		return d.appendRuns(nil, states, since, items...)
	}

	var lastKey map[string]*dynamodb.AttributeValue
	for {
		input := &dynamodb.QueryInput{
			TableName:              aws.String(d.table),
			KeyConditionExpression: aws.String(`#study = :study AND #run > :zero`),
			FilterExpression:       aws.String(`#state <> :pending OR #keepalive > :min_pending_time`),
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":study":            {S: aws.String(study)},
				":zero":             {N: aws.String("0")},
				":pending":          {S: aws.String("pending")},
				":min_pending_time": {S: aws.String(minPendingTime.UTC().Format(time.RFC3339))},
			},
			ExpressionAttributeNames: appendAttributeNames(nil, "study", "run", "state", "keepalive"),
		}
		if lastKey != nil {
			input.ExclusiveStartKey = lastKey
		}
		out, err := d.db.QueryWithContext(ctx, input)
		if err != nil {
			return nil, err
		}
		runs, err = d.appendRuns(runs, states, since, out.Items...)
		if err != nil {
			return nil, err
		}
		lastKey = out.LastEvaluatedKey
		if lastKey == nil {
			break
		}
	}
	return
}

// Run implements diviner.Database.
func (d *DB) Run(ctx context.Context, study, id string) (diviner.Run, error) {
	log.Debug.Printf("dydb.Run: %s %s", study, id)
	seq, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid run key %q: %v", id, err)
	}
	r := &run{sess: d.sess, db: d.db, table: d.table, seq: seq, studyName: study}
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
		ConditionExpression: aws.String(`attribute_not_exists(#study)`),
		Item: map[string]*dynamodb.AttributeValue{
			"study":       {S: aws.String(study.Name)},
			"run":         {N: aws.String("0")},
			"num_studies": {N: aws.String("0")},
			"meta":        {B: b.Bytes()},
		},
		ExpressionAttributeNames: appendAttributeNames(nil, "study"),
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
		UpdateExpression: aws.String(`SET #num_studies = #num_studies + :one`),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":one": {N: aws.String("1")},
		},
		ExpressionAttributeNames: appendAttributeNames(nil, "num_studies"),
		ReturnValues:             aws.String(`UPDATED_NEW`),
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

func (d *DB) querySince(ctx context.Context, since time.Time, newQuery func() *dynamodb.QueryInput) ([]map[string]*dynamodb.AttributeValue, error) {
	var queries []*dynamodb.QueryInput
	for _, t := range dates(since, time.Now()) {
		query := newQuery()
		query.TableName = aws.String(d.table)
		query.IndexName = aws.String(keepaliveIndexName)
		query.KeyConditionExpression = aws.String(`#date = :date AND #keepalive > :since`)
		query.ExpressionAttributeNames = appendAttributeNames(query.ExpressionAttributeNames, "date", "keepalive")
		if query.ExpressionAttributeValues == nil {
			query.ExpressionAttributeValues = make(map[string]*dynamodb.AttributeValue)
		}
		query.ExpressionAttributeValues[":date"] = &dynamodb.AttributeValue{S: aws.String(t.UTC().Format(dateLayout))}
		query.ExpressionAttributeValues[":since"] = &dynamodb.AttributeValue{S: aws.String(since.UTC().Format(timeLayout))}
		queries = append(queries, query)
	}
	itemss := make([][]map[string]*dynamodb.AttributeValue, len(queries))
	err := traverse.Each(len(queries), func(i int) error {
		var (
			query   = queries[i]
			lastKey map[string]*dynamodb.AttributeValue
		)
		for {
			if lastKey != nil {
				query.ExclusiveStartKey = lastKey
			}
			out, err := d.db.QueryWithContext(ctx, query)
			if err != nil {
				return err
			}
			itemss[i] = append(itemss[i], out.Items...)
			lastKey = out.LastEvaluatedKey
			if lastKey == nil {
				break
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	var items []map[string]*dynamodb.AttributeValue
	for i := range itemss {
		items = append(items, itemss[i]...)
	}
	return items, nil
}

func (d *DB) appendRuns(runs []diviner.Run, states diviner.RunState, since time.Time, items ...map[string]*dynamodb.AttributeValue) ([]diviner.Run, error) {
	minPendingTime := time.Now().Add(-2 * keepaliveInterval)
	for _, item := range items {
		r := &run{sess: d.sess, db: d.db, table: d.table}
		if err := r.unmarshal(item); err != nil {
			return runs, err
		}
		if r.State() == diviner.Pending && r.Updated().Before(minPendingTime) {
			continue
		}
		if r.Updated().Before(since) {
			continue
		}
		if r.State()&states == r.State() {
			runs = append(runs, r)
		}
	}
	return runs, nil
}

// A run is a single diviner run. It implements diviner.Run on top of dynamodb.
type run struct {
	sess      *session.Session
	db        *dynamodb.DynamoDB
	table     string
	values    diviner.Values
	studyName string
	seq       uint64
	state     diviner.RunState
	config    diviner.RunConfig
	created   time.Time
	updated   time.Time
	runtime   time.Duration
	cancel    func()

	once   sync.Once
	writec chan []byte
	flushc chan chan error

	cloudwatchOnce once.Task
	logs           *cloudwatchlogs.CloudWatchLogs
	logsSeq        *string

	statusc chan string
}

func (r *run) String() string {
	return r.ID()
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
	if state := attrs["state"]; state == nil || state.S == nil {
		return errors.New("missing state")
	}
	if timestamp := attrs["timestamp"]; timestamp == nil || timestamp.S == nil {
		return errors.New("missing timestamp")
	}
	r.studyName = *attrs["study"].S
	var err error
	r.seq, err = strconv.ParseUint(*attrs["run"].N, 10, 64)
	if err != nil {
		return err
	}
	if err := gob.NewDecoder(bytes.NewReader(attrs["values"].B)).Decode(&r.values); err != nil {
		return err
	}
	switch state := *attrs["state"].S; state {
	case "pending":
		r.state = diviner.Pending
	case "success":
		r.state = diviner.Success
	case "failure":
		r.state = diviner.Failure
	default:
		log.Printf("run %s has unknown state %s", r, state)
		r.state = 0
	}
	r.created, err = time.Parse(timeLayout, *attrs["timestamp"].S)
	if err != nil {
		return err
	}
	r.updated, err = time.Parse(timeLayout, *attrs["keepalive"].S)
	if err != nil {
		return err
	}
	// Backwards compatibilty: set an empty config where it doesn't exist.
	if attrs["config"] != nil && attrs["config"].B != nil {
		if err := gob.NewDecoder(bytes.NewReader(attrs["config"].B)).Decode(&r.config); err != nil {
			return err
		}
	}
	// If we have a runtime, use it, otherwise we subtract
	// the last keepalive time if we have it.
	if attrs["runtime"] != nil && attrs["runtime"].S != nil {
		r.runtime, err = time.ParseDuration(*attrs["runtime"].S)
		if err != nil {
			return err
		}
	} else if attrs["keepalive"] != nil && attrs["keepalive"].S != nil {
		lastKeepalive, err := time.Parse(timeLayout, *attrs["keepalive"].S)
		if err != nil {
			return err
		}
		r.runtime = lastKeepalive.Sub(r.created)
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
	return fmt.Sprint(r.seq)
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
		UpdateExpression: aws.String(`SET #metrics = list_append(#metrics, :metrics)`),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":metrics": {L: []*dynamodb.AttributeValue{metricsValue(metrics)}},
		},
		ExpressionAttributeNames: appendAttributeNames(nil, "metrics"),
	})
	return err
}

// SetStatus implemnets diviner.Run.
func (r *run) SetStatus(ctx context.Context, status string) error {
	for {
		select {
		case r.statusc <- status:
			return nil
		case <-r.statusc:
		}
	}
}

// Status implements diviner.Run.
func (r *run) Status(ctx context.Context) (string, error) {
	out, err := r.db.GetItemWithContext(ctx, &dynamodb.GetItemInput{
		TableName:                aws.String(r.table),
		Key:                      r.key(),
		ProjectionExpression:     aws.String("#status"),
		ExpressionAttributeNames: appendAttributeNames(nil, "status"),
	})
	if err != nil {
		return "", err
	}
	status := out.Item["status"]
	if status == nil {
		return "", nil
	}
	return aws.StringValue(status.S), nil
}

// Created implements diviner.Run.
func (r *run) Created() time.Time {
	return r.created
}

func (r *run) Updated() time.Time {
	return r.updated
}

// Runtime implements diviner.Run.
func (r *run) Runtime() time.Duration {
	return r.runtime
}

// Config implements diviner.Run.
func (r *run) Config() diviner.RunConfig {
	return r.config
}

// Values implements diviner.Run.
func (r *run) Values() diviner.Values {
	return r.values
}

// Metrics implements diviner.Run.
func (r *run) Metrics(ctx context.Context) (metrics diviner.Metrics, err error) {
	out, err := r.db.GetItemWithContext(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(r.table),
		Key:       r.key(),
	})
	if err != nil {
		return nil, err
	}
	list := out.Item["metrics"].L
	if len(list) == 0 {
		return nil, nil
	}
	return valueMetrics(list[len(list)-1])
}

// Complete implements diviner.Run.
func (r *run) Complete(ctx context.Context, state diviner.RunState, runtime time.Duration) error {
	_, err := r.db.UpdateItemWithContext(ctx, &dynamodb.UpdateItemInput{
		TableName:        aws.String(r.table),
		Key:              r.key(),
		UpdateExpression: aws.String(`SET #state = :state, #runtime = :runtime`),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":state":   {S: aws.String(state.String())},
			":runtime": {S: aws.String(runtime.String())},
		},
		ExpressionAttributeNames: appendAttributeNames(nil, "state", "runtime"),
	})
	if err == nil {
		r.state = state
	}
	r.flusher(true)
	r.cancel()
	return err
}

// Log implements diviner.Run.
func (r *run) Log(follow bool) io.Reader {
	group, stream := r.streamKeys()
	return &logReader{sess: r.sess, group: group, stream: stream, follow: follow}
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
			if err == nil {
				events = nil
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
		log.Printf("dydb: created cloudwatch stream, group %s, name %s", group, stream)
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
		var seq string
		if r.logsSeq != nil {
			seq = *r.logsSeq
		}
		log.Error.Printf("CloudWatchLogs.PutLogEvent(seq: %v): %v", seq, err)
		// Clear the sequence, in case the error is due to missynchronized sequence
		// tokens.  This could happens when two diviner instances are writing to the
		// same stream due to external race.
		r.logsSeq = nil
	} else {
		r.logsSeq = out.NextSequenceToken
	}
	return err
}

func (r *run) streamKeys() (group, stream string) {
	studyName := strings.Replace(r.studyName, ",", "/", -1)
	studyName = strings.Replace(studyName, "=", "_", -1)
	return fmt.Sprintf("%s/%s", r.table, studyName), fmt.Sprint(r.seq)
}

func (r *run) key() map[string]*dynamodb.AttributeValue {
	return map[string]*dynamodb.AttributeValue{
		"study": {S: aws.String(r.studyName)},
		"run":   {N: aws.String(fmt.Sprint(r.seq))},
	}
}

func (r *run) keepalive(ctx context.Context) {
	tick := time.NewTicker(keepaliveInterval / 2)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
		case <-ctx.Done():
			return
		}
		var (
			now       = time.Now().UTC()
			keepalive = now.Format(timeLayout)
			date      = now.Format(dateLayout)
		)
		input := &dynamodb.UpdateItemInput{
			TableName:        aws.String(r.table),
			Key:              r.key(),
			UpdateExpression: aws.String(`SET #keepalive = :timestamp, #date = :date`),
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":timestamp": {S: aws.String(keepalive)},
				":date":      {S: aws.String(date)},
			},
			ExpressionAttributeNames: appendAttributeNames(nil, "keepalive", "date"),
		}
		if _, err := r.db.UpdateItemWithContext(ctx, input); err != nil {
			log.Error.Printf("run %s: failed to update keepalive timestamp: %v", r, err)
		}
		// Also update the study:
		input.Key["run"] = &dynamodb.AttributeValue{N: aws.String("0")}
		if _, err := r.db.UpdateItemWithContext(ctx, input); err != nil {
			log.Error.Printf("run %s: failed to update study keepalive timestamp: %v", r, err)
		}
	}
}

func (r *run) updater(ctx context.Context) {
	limiter := rate.NewLimiter(rate.Every(10*time.Second), 2)
	for {
		var status string
		select {
		case <-ctx.Done():
			return
		case status = <-r.statusc:
		}
		if err := limiter.Wait(ctx); err != nil {
			return
		}
		// We may have a new status by now.
		select {
		case status = <-r.statusc:
		default:
		}
		_, err := r.db.UpdateItemWithContext(ctx, &dynamodb.UpdateItemInput{
			TableName:        aws.String(r.table),
			Key:              r.key(),
			UpdateExpression: aws.String(`SET #status = :status`),
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":status": {S: aws.String(status)},
			},
			ExpressionAttributeNames: appendAttributeNames(nil, "status"),
		})
		if err != nil {
			log.Error.Printf("run %s: failed to set status %s: %v", r, status, err)
		}
	}
}

func metricsValue(m diviner.Metrics) *dynamodb.AttributeValue {
	v := new(dynamodb.AttributeValue)
	v.M = make(map[string]*dynamodb.AttributeValue)
	for k, n := range m {
		// DynamoDB does not support storing NaNs, so we must omit them.
		// TODO(marius): should we store the NaNs explicitly in some other way?
		if math.IsNaN(n) {
			log.Error.Printf("dynamodb: dropping metric %s: NaN", k)
			continue
		}
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

var errEmptyReply = errors.New("empty reply")

type logReader struct {
	sess          *session.Session
	group, stream string

	follow    bool
	save, buf []byte
	nextToken *string
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

// AttributeNames appends requested attribute names to attrs and
// returns it. If attrs is nil, a new map is created.
func appendAttributeNames(attrs map[string]*string, newAttrs ...string) map[string]*string {
	if attrs == nil {
		attrs = make(map[string]*string)
	}
	for _, attr := range newAttrs {
		attrs["#"+attr] = aws.String(attr)
	}
	return attrs
}

func appendStudies(studies []diviner.Study, items ...map[string]*dynamodb.AttributeValue) []diviner.Study {
	for _, item := range items {
		var study diviner.Study
		p := item["meta"].B
		if err := gob.NewDecoder(bytes.NewReader(p)).Decode(&study); err != nil {
			log.Error.Printf("skipping invalid study %s: %v", aws.StringValue(item["study"].S), err)
			continue
		}
		studies = append(studies, study)
	}
	return studies
}
