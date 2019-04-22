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
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/traverse"
	"github.com/grailbio/diviner"
	"github.com/grailbio/diviner/dydb/dynamoattr"
)

// TODO(marius): get rid of using gob here; either encode data directly in dynamoDB
// attributes or use JSON.

const (
	// The run keepalive interval used to query for live runs.
	keepaliveInterval = 30 * time.Second

	// The interval between updating study keepalives.
	studyKeepaliveInterval = 30 * time.Minute

	// The time layout used to store timestamps in dynamodb.
	// RFC3339 timestamps order lexically.
	timeLayout = time.RFC3339

	// DateLayout is used to partition the keepalive index.
	dateLayout = "2006-01-02"

	// ScanSegments is the number of concurrent scan operations we perform.
	scanSegments = 50

	keepaliveIndexName = "date-keepalive-index"
)

// A DB represents a session to a DynamoDB table; it implements
// diviner.Database.
type DB struct {
	sess  *session.Session
	db    *dynamodb.DynamoDB
	table string

	mu                 sync.Mutex
	lastStudyKeepalive map[string]time.Time
}

// New creates a new DB instance from the provided session and table name.
func New(sess *session.Session, table string) *DB {
	return &DB{
		sess:               sess,
		db:                 dynamodb.New(sess),
		table:              table,
		lastStudyKeepalive: make(map[string]time.Time),
	}
}

// CreateStudyIfNotExist creates a new study if it does not already exist.
// Existing studies are not updated.
func (d *DB) CreateStudyIfNotExist(ctx context.Context, study diviner.Study) (created bool, err error) {
	var b bytes.Buffer
	if err := gob.NewEncoder(&b).Encode(study); err != nil {
		return false, err
	}
	input := &dynamodb.PutItemInput{
		TableName:           aws.String(d.table),
		ConditionExpression: aws.String(`attribute_not_exists(#study)`),
		Item: map[string]*dynamodb.AttributeValue{
			"study":       {S: aws.String(study.Name)},
			"run":         {N: aws.String("0")},
			"num_studies": {N: aws.String("0")},
			"meta":        {B: b.Bytes()},
		},
		ExpressionAttributeNames: appendAttributeNames(nil, "study"),
	}
	_, err = d.db.PutItemWithContext(ctx, input)
	debug("dynamodb.PutItem", input, nil, err)
	if err != nil {
		aerr, ok := err.(awserr.Error)
		if !ok || aerr.Code() != "ConditionalCheckFailedException" {
			return false, err
		}
		return false, nil
	}
	return true, nil
}

// LookupStudy returns the study with the provided name.
func (d *DB) LookupStudy(ctx context.Context, name string) (study diviner.Study, err error) {
	input := &dynamodb.GetItemInput{
		TableName: aws.String(d.table),
		Key: map[string]*dynamodb.AttributeValue{
			"study": {S: aws.String(name)},
			"run":   {N: aws.String("0")},
		},
	}
	out, err := d.db.GetItemWithContext(ctx, input)
	debug("dynamodb.GetItem", input, out, err)
	if err != nil {
		return
	}
	if meta := out.Item["meta"]; meta == nil || meta.B == nil {
		return diviner.Study{}, diviner.ErrNotExist
	}
	err = gob.NewDecoder(bytes.NewReader(out.Item["meta"].B)).Decode(&study)
	return
}

// ListStudies returns the set of studies in the database that have the provided
// prefix and have been active since the provided time.
func (d *DB) ListStudies(ctx context.Context, prefix string, since time.Time) ([]diviner.Study, error) {
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
		out, err := d.db.ScanWithContext(ctx, input)
		debug("dynamodb.Scan", input, out, err)
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

// InsertRun inserts a new run into the provided study. The returned run is
// assigned a fresh sequence number and is returned with state Pending.
func (d *DB) InsertRun(ctx context.Context, run diviner.Run) (diviner.Run, error) {
	if run.Seq == 0 {
		var err error
		run.Seq, err = d.NextSeq(ctx, run.Study)
		if err != nil {
			return diviner.Run{}, err
		}
	}
	run.State = diviner.Pending
	run.Status = ""
	run.Created = time.Now()
	run.Updated = run.Created
	run.Runtime = 0
	run.Metrics = nil
	attrs, err := marshal(run)
	if err != nil {
		return diviner.Run{}, err
	}
	putInput := &dynamodb.PutItemInput{
		TableName: aws.String(d.table),
		Item:      attrs,
	}
	_, err = d.db.PutItem(putInput)
	debug("dynamodb.PutItem", putInput, nil, err)
	if err != nil {
		return diviner.Run{}, err
	}
	d.keepaliveStudy(ctx, run.Study)
	return run, nil
}

// UpdateRun updates the state, message, and runtime of the run named by the provided
// study and sequence number.
func (d *DB) UpdateRun(ctx context.Context, study string, seq uint64, state diviner.RunState, message string, runtime time.Duration) error {
	if message == "" {
		// The dynamoDB API does not allow for empty string values.
		message = "(none)"
	}
	now := time.Now()
	input := &dynamodb.UpdateItemInput{
		TableName:        aws.String(d.table),
		Key:              key(study, seq),
		UpdateExpression: aws.String(`SET #status = :status, #state = :state, #runtime = :runtime, #keepalive = :timestamp, #date = :date`),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":status":    {S: aws.String(message)},
			":state":     {S: aws.String(state.String())},
			":runtime":   {S: aws.String(runtime.String())},
			":timestamp": {S: aws.String(now.UTC().Format(timeLayout))},
			":date":      {S: aws.String(now.UTC().Format(dateLayout))},
		},
		ExpressionAttributeNames: appendAttributeNames(nil, "status", "state", "runtime", "keepalive", "date"),
	}
	_, err := d.db.UpdateItemWithContext(ctx, input)
	debug("dynamodb.UpdateItem", input, nil, err)
	d.keepaliveStudy(ctx, study)
	return err
}

// AppendRunMetrics reports new run metrics for the run named by the provided
// study and sequence number.
func (d *DB) AppendRunMetrics(ctx context.Context, study string, seq uint64, metrics diviner.Metrics) error {
	input := &dynamodb.UpdateItemInput{
		TableName:        aws.String(d.table),
		Key:              key(study, seq),
		UpdateExpression: aws.String(`SET #metrics = list_append(#metrics, :metrics)`),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":metrics": {L: []*dynamodb.AttributeValue{metricsValue(metrics)}},
		},
		ExpressionAttributeNames: appendAttributeNames(nil, "metrics"),
	}
	_, err := d.db.UpdateItemWithContext(ctx, input)
	debug("dynamodb.UpdateItem", input, nil, err)
	return err
}

// ListRuns returns all runs in the provided study matching the query states that
// have also been active since the provided time.
func (d *DB) ListRuns(ctx context.Context, study string, states diviner.RunState, since time.Time) (runs []diviner.Run, err error) {
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
		return d.appendRuns(nil, study, states, since, items...)
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
		debug("dynamodb.Query", input, out, err)
		if err != nil {
			return nil, err
		}
		runs, err = d.appendRuns(runs, study, states, since, out.Items...)
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

// LookupRun retruns the run named by the provided study and sequence number.
func (d *DB) LookupRun(ctx context.Context, study string, seq uint64) (diviner.Run, error) {
	input := &dynamodb.GetItemInput{
		TableName: aws.String(d.table),
		Key:       key(study, seq),
	}
	out, err := d.db.GetItem(input)
	debug("dynamodb.GetItem", input, out, err)
	if err != nil {
		return diviner.Run{}, err
	}
	return unmarshal(out.Item)
}

// keepaliveStudy update's the study's update time. Concurrent calls for
// a single study are coalesced.
//
// TODO(marius): we don't need to keepalive studies as aggressively
// as we do runs, since they are used for much coarser grained querying
// and filtering.
func (d *DB) keepaliveStudy(ctx context.Context, study string) {
	now := time.Now()
	d.mu.Lock()
	if now.Sub(d.lastStudyKeepalive[study]) < studyKeepaliveInterval {
		d.mu.Unlock()
		return
	}
	prev := d.lastStudyKeepalive[study]
	d.lastStudyKeepalive[study] = now
	d.mu.Unlock()
	go func() {
		input := &dynamodb.UpdateItemInput{
			TableName: aws.String(d.table),
			Key: map[string]*dynamodb.AttributeValue{
				"study": {S: aws.String(study)},
				"run":   {N: aws.String("0")},
			},
			UpdateExpression: aws.String(`SET #keepalive = :timestamp, #date = :date`),
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":timestamp": {S: aws.String(now.UTC().Format(timeLayout))},
				":date":      {S: aws.String(now.UTC().Format(dateLayout))},
			},
			ExpressionAttributeNames: appendAttributeNames(nil, "keepalive", "date"),
		}
		_, err := d.db.UpdateItemWithContext(ctx, input)
		debug("dynamodb.UpdateItem", input, nil, err)
		if err == nil {
			return
		}
		d.mu.Lock()
		d.lastStudyKeepalive[study] = prev
		d.mu.Unlock()
		log.Error.Printf("failed to update keepalive for study %s: %v", study, err)
	}()
}

// NextSeq reserves the next run ID for the provided study.
func (d *DB) NextSeq(ctx context.Context, study string) (uint64, error) {
	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(d.table),
		Key: map[string]*dynamodb.AttributeValue{
			"study": {S: aws.String(study)},
			"run":   {N: aws.String("0")},
		},
		UpdateExpression: aws.String(`SET #num_studies = #num_studies + :one`),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":one": {N: aws.String("1")},
		},
		ExpressionAttributeNames: appendAttributeNames(nil, "num_studies"),
		ReturnValues:             aws.String(`UPDATED_NEW`),
	}
	out, err := d.db.UpdateItemWithContext(ctx, input)
	debug("dynamodb.UpdateItem", input, out, err)
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
			debug("dynamodb.Query", query, out, err)
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

func (d *DB) appendRuns(runs []diviner.Run, study string, states diviner.RunState, since time.Time, items ...map[string]*dynamodb.AttributeValue) ([]diviner.Run, error) {
	minPendingTime := time.Now().Add(-2 * keepaliveInterval)
	for i, item := range items {
		run, err := unmarshal(item)
		if err != nil {
			log.Error.Printf("dropping run %d of study %s unmarshal: %v", i, study, err)
			continue
		}
		if run.State == diviner.Pending && run.Updated.Before(minPendingTime) {
			continue
		}
		if run.Updated.Before(since) {
			continue
		}
		if run.State&states == run.State {
			runs = append(runs, run)
		}
	}
	return runs, nil
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

// DynamodbRun is used to bridge the dynamodb representation of a run with
// that of diviner.Run.
type dynamodbRun struct {
	Study     string            `dynamoattr:"study"`
	Seq       uint64            `dynamoattr:"run"`
	Values    []byte            `dynamoattr:"values"`
	Metrics   []diviner.Metrics `dynamoattr:"metrics"`
	State     string            `dynamoattr:"state"`
	Status    string            `dynamoattr:"status"`
	Created   string            `dynamoattr:"timestamp"`
	Runtime   string            `dynamoattr:"runtime"`
	Keepalive string            `dynamoattr:"keepalive"`
	Date      string            `dynamoattr:"date"`
	Config    []byte            `dynamoattr:"config"`
}

func marshal(run diviner.Run) (map[string]*dynamodb.AttributeValue, error) {
	var dyrun dynamodbRun
	dyrun.Study = run.Study
	dyrun.Seq = run.Seq
	b := new(bytes.Buffer)
	if err := gob.NewEncoder(b).Encode(run.Values); err != nil {
		return nil, err
	}
	dyrun.Values = b.Bytes()
	dyrun.Metrics = run.Metrics
	if dyrun.Metrics == nil {
		dyrun.Metrics = []diviner.Metrics{}
	}
	dyrun.State = run.State.String()
	dyrun.Status = run.Status
	dyrun.Created = run.Created.UTC().Format(timeLayout)
	dyrun.Runtime = run.Runtime.String()
	dyrun.Keepalive = run.Updated.UTC().Format(timeLayout)
	dyrun.Date = run.Updated.UTC().Format(dateLayout)
	b = new(bytes.Buffer)
	if err := gob.NewEncoder(b).Encode(run.Config); err != nil {
		return nil, err
	}
	dyrun.Config = b.Bytes()
	return dynamoattr.Marshal(dyrun)
}

func unmarshal(attrs map[string]*dynamodb.AttributeValue) (diviner.Run, error) {
	var dyrun dynamodbRun
	if err := dynamoattr.Unmarshal(attrs, &dyrun); err != nil {
		return diviner.Run{}, err
	}
	var run diviner.Run
	run.Study = dyrun.Study
	run.Seq = dyrun.Seq
	if err := gob.NewDecoder(bytes.NewReader(dyrun.Values)).Decode(&run.Values); err != nil {
		return diviner.Run{}, errors.E("decode values", err)
	}
	run.Metrics = dyrun.Metrics
	switch dyrun.State {
	case "pending":
		run.State = diviner.Pending
	case "success":
		run.State = diviner.Success
	case "failure":
		run.State = diviner.Failure
	default:
		return diviner.Run{}, fmt.Errorf("invalid run state %s", dyrun.State)
	}
	run.Status = dyrun.Status
	var err error
	if run.Created, err = time.Parse(timeLayout, dyrun.Created); err != nil {
		return diviner.Run{}, err
	}
	if run.Updated, err = time.Parse(timeLayout, dyrun.Keepalive); err != nil {
		return diviner.Run{}, err
	}
	// Ignore dyrun.Date; this is just for indexing.
	if dyrun.Runtime != "" {
		run.Runtime, err = time.ParseDuration(dyrun.Runtime)
		if err != nil {
			return diviner.Run{}, err
		}
	} else if run.State == diviner.Pending {
		run.Runtime = time.Since(run.Created)
	}

	if err := gob.NewDecoder(bytes.NewReader(dyrun.Config)).Decode(&run.Config); err != nil {
		return diviner.Run{}, errors.E("decode config", err)
	}
	return run, nil
}

func key(study string, seq uint64) map[string]*dynamodb.AttributeValue {
	return map[string]*dynamodb.AttributeValue{
		"study": {S: aws.String(study)},
		"run":   {N: aws.String(fmt.Sprint(seq))},
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

func debug(method string, input, output interface{}, err error) {
	if !log.At(log.Debug) {
		return
	}
	if err != nil {
		log.Debug.Printf("%s %s: %v", method, input, err)
	} else if output != nil {
		log.Debug.Printf("%s %s: %s", method, input, output)
	} else {
		log.Debug.Printf("%s %s", method, input)
	}
}
