package importer

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/smithy-go/ptr"
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
	// TODO: Add nil check (temporarily unnecessary because the struct is not expected to be used by external projects)
	if c := *a.concurrency; c < 1 || c > 25 {
		return a, fmt.Errorf("concurrency (c) needs to fill: 0 < c <= 25, but was %d", c)
	}
	return a, nil
}

func (a App) Run(ctx context.Context) error {
	// TODO: implement this
	panic("implement me!")
}
