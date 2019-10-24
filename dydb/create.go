// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package dydb

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/log"
)

// CreateTable creates the dynamoDB table named by this DB instance
// together with the required indices for operating in Diviner. The billing
// mode is set to PAY_PER_REQUEST.
func (d *DB) CreateTable(ctx context.Context) error {
	out, err := d.db.CreateTableWithContext(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(d.table),
		KeySchema: []*dynamodb.KeySchemaElement{
			{AttributeName: aws.String("study"), KeyType: aws.String("HASH")},
			{AttributeName: aws.String("run"), KeyType: aws.String("RANGE")},
		},
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{AttributeName: aws.String("date"), AttributeType: aws.String("S")},
			{AttributeName: aws.String("keepalive"), AttributeType: aws.String("S")},
			{AttributeName: aws.String("run"), AttributeType: aws.String("N")},
			{AttributeName: aws.String("study"), AttributeType: aws.String("S")},
		},
		GlobalSecondaryIndexes: []*dynamodb.GlobalSecondaryIndex{{
			IndexName: aws.String(keepaliveIndexName),
			KeySchema: []*dynamodb.KeySchemaElement{
				{AttributeName: aws.String("date"), KeyType: aws.String("HASH")},
				{AttributeName: aws.String("keepalive"), KeyType: aws.String("RANGE")},
			},
			Projection: &dynamodb.Projection{
				ProjectionType: aws.String("ALL"),
			},
		}},
		BillingMode: aws.String("PAY_PER_REQUEST"),
	})
	if err != nil {
		return err
	}
	log.Printf("dynamodb: created table %s: waiting for it to become ACTIVE", d.table)
	for start := time.Now(); ; {
		describe, err := d.db.DescribeTable(&dynamodb.DescribeTableInput{TableName: aws.String(d.table)})
		if err != nil {
			return err
		}
		status := *describe.Table.TableStatus
		if status == "ACTIVE" {
			break
		}
		if time.Since(start) > time.Minute {
			return errors.New("waited for table to become active for too long; try again later")
		}
		time.Sleep(4 * time.Second)
	}
	log.Print(out.TableDescription)
	return nil
}
