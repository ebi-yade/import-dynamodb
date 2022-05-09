package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/hashicorp/logutils"

	"github.com/ebi-yade/frog"

	importer "github.com/ebi-yade/import-dynamodb"
)

func main() {
	if err := entrypoint(); err != nil {
		log.Println("[ERROR]", err)
		os.Exit(1)
	}
}

var (
	filter = &logutils.LevelFilter{
		Levels:   []logutils.LogLevel{"DEBUG", "INFO"},
		MinLevel: logutils.LogLevel("INFO"),
		Writer:   os.Stderr,
	}
)

func entrypoint() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if debug, _ := strconv.ParseBool(os.Getenv("DEBUG")); debug {
		filter.MinLevel = logutils.LogLevel("DEBUG")
	}
	log.SetOutput(filter)

	var (
		manifestBucket *string
		manifestKey    *string
		tableName      *string
		concurrency    *int
	)

	frog.StringVar(manifestBucket, "manifest-bucket", "S3 bucket to the manifest file")
	frog.StringVar(manifestKey, "manifest-key", "S3 key to the manifest file")
	frog.StringVar(tableName, "table-name", "DynamoDB table name to be restored")
	frog.IntVar(concurrency, "concurrency", "max concurrency of BatchWriteItem process (no more than 25)")
	frog.Parse()

	aws, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("failed to load AWS config at entrypoint(): %w", err)
	}

	app, err := importer.NewApp(aws, manifestBucket, manifestKey, tableName).SetConcurrency(concurrency).Validate()
	if err != nil {
		return fmt.Errorf("failed to configure App: %w", err)
	}
	return app.Run(ctx)
}
