package importer

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/ptr"
	"go.uber.org/multierr"
)

type App struct {
	AWS aws.Config

	manifestBucket *string
	manifestKey    *string
	tableName      *string
	concurrency    *int
}

func loadApp(aws aws.Config) *App {
	app := &App{
		AWS: aws,
	}

	if bucket, ok := os.LookupEnv("MANIFEST_S3_BUCKET"); ok {
		app.manifestBucket = &bucket
	}
	if key, ok := os.LookupEnv("MANIFEST_S3_KEY"); ok {
		app.manifestKey = &key
	}
	if table, ok := os.LookupEnv("TABLE_NAME"); ok {
		app.tableName = &table
	}

	if concurrency, ok := os.LookupEnv("CONCURRENCY"); ok {
		i64, err := strconv.ParseInt(concurrency, 0, strconv.IntSize)
		if err != nil {
			log.Printf("[DEBUG] environment variable 'CONCURRENCY' is detected, but not a number, so the value %s is ignored\n", concurrency)
		} else {
			app.concurrency = ptr.Int(int(i64))
		}
	}

	return app
}

func NewApp(aws aws.Config, bucket *string, key *string, table *string) *App {
	app := loadApp(aws)

	if bucket != nil {
		app.manifestBucket = bucket
	}
	if key != nil {
		app.manifestKey = key
	}
	if table != nil {
		app.tableName = table
	}

	return app
}

func (a *App) SetConcurrency(max *int) *App {
	if max != nil {
		a.concurrency = max
	}
	return a
}

func (a *App) Validate() (*App, error) {
	var ers error
	if a.manifestBucket == nil {
		ers = multierr.Append(ers, fmt.Errorf("the bucket name of manifest file on S3 is required, but not set"))
	}
	if a.manifestKey == nil {
		ers = multierr.Append(ers, fmt.Errorf("the key name of manifest file on S3 is required, but not set"))
	}
	if a.tableName == nil {
		ers = multierr.Append(ers, fmt.Errorf("the table name of DynamoDB to import data into is required, but not set"))
	}
	if a.concurrency == nil {
		log.Println("[DEBUG] concurrency is not specified, then set to default (25)")
		a.concurrency = ptr.Int(25)
	}
	if ers != nil {
		return a, ers
	}

	if c := *a.concurrency; c < 1 || c > 25 {
		return a, fmt.Errorf("concurrency (c) needs to fill: 0 < c <= 25, but was %d", c)
	}
	return a, nil
}

func (a App) Run(ctx context.Context) error {
	ddbClient := dynamodb.NewFromConfig(a.AWS)
	_, err := ddbClient.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: a.tableName,
	})
	if err != nil {
		return fmt.Errorf("failed to find the DynamoDB table: %s: %w", *a.tableName, err)
	}

	s3Client := s3.NewFromConfig(a.AWS)
	bucket := a.manifestBucket
	summary, err := loadSummary(ctx, s3Client, bucket, a.manifestKey)
	if err != nil {
		return fmt.Errorf("failed to load the manifest summary file: %w", err)
	}

	manifests, err := loadManifests(ctx, s3Client, bucket, summary.ManifestFilesS3Key)
	if err != nil {
		return fmt.Errorf("failed to load the manifest file: %w", err)
	}

	for _, man := range manifests {
		log.Println("data S3 key:", *man.DataFileS3Key)
	}
	return nil
}
