package importer

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.uber.org/multierr"

	"github.com/ebi-yade/import-dynamodb/convert"
)

type row struct {
	Item map[string]events.DynamoDBAttributeValue `json:"Item"`
}

const limitBatchWriteItems = 25

func (a App) importByManifest(ctx context.Context, manifest Manifest, ddb DDB) error {
	log.Printf("[INFO] importing data via s3://%s/%s\n", *a.manifestBucket, *manifest.DataFileS3Key)
	ctxImport, cancel := context.WithCancel(ctx)
	defer cancel()

	batchData := make(chan []types.WriteRequest, *a.concurrency)
	batchDone := make(chan error)
	go func() {
		a.batch(ctxImport, batchData, batchDone)
	}()

	if err := a.invoke(ctxImport, manifest, ddb, batchData); err != nil {
		cancel()
		return fmt.Errorf("error in the invoke process: %w", err)
	}

	if err := <-batchDone; err != nil {
		return fmt.Errorf("error in the batch process %w", err)
	}
	return nil
}

func (a App) invoke(ctx context.Context, manifest Manifest, ddb DDB, batchData chan []types.WriteRequest) error {
	data, err := a.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: a.manifestBucket,
		Key:    manifest.DataFileS3Key,
	})
	if err != nil {
		return fmt.Errorf("failed to get the file from s3://%s/%s: %w", *a.manifestBucket, *manifest.DataFileS3Key, err)
	}
	reader, err := gzip.NewReader(data.Body)
	if err != nil {
		return fmt.Errorf("failed to create gzip reader: %w", err)
	}
	defer reader.Close()

	store := make(map[string][]types.WriteRequest)

	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		if len(strings.TrimSpace(scanner.Text())) == 0 {
			continue
		}
		var r row
		if err := json.Unmarshal(scanner.Bytes(), &r); err != nil {
			return fmt.Errorf("failed to parse JSON as DynamoDB Event: %w", err)
		}
		item, err := convert.FromDynamoDBEventAVMap(r.Item)
		if err != nil {
			return fmt.Errorf("failed to convert events.DynamoDBAttributeValue to types.AttributeValue")
		}

		key, err := convert.Stringify(r.Item[ddb.hashKey])
		if err != nil {
			return fmt.Errorf("failed to convert r.Item[hashKey] into a string value: %w", err)
		}

		cache, ok := store[key]
		if !ok {
			cache = make([]types.WriteRequest, 0, limitBatchWriteItems)
		}
		cache = append(cache, types.WriteRequest{PutRequest: &types.PutRequest{Item: item}})
		if len(cache) < limitBatchWriteItems && len(cache) != rand.Intn(limitBatchWriteItems) {
			store[key] = cache
		} else {
			batchData <- cache
			delete(store, key)
		}
	}

	// flush stored data at the end
	buffer := make([]types.WriteRequest, 0, limitBatchWriteItems)
	for _, items := range store {
		if len(buffer)+len(items) > limitBatchWriteItems {
			batchData <- buffer
			buffer = buffer[:0]
		}
		buffer = append(buffer, items...)
	}
	batchData <- buffer
	close(batchData)
	return nil
}

func (a App) batch(ctx context.Context, batchData chan []types.WriteRequest, writeDone chan error) {
	wg := &sync.WaitGroup{}
	var ers error
	for i := 0; i < *a.concurrency; i++ {
		wg.Add(1)
		go func(ctx context.Context, id int, batchData chan []types.WriteRequest) {
			defer wg.Done()
			log.Printf("[DEBUG] starting the batch (processID: %d)\n", id)
			for {
				select {
				case reqs, ok := <-batchData:
					if !ok {
						log.Printf("[DEBUG] closed channel detected (processID: %d)\n", id)
						return
					}
					if len(reqs) > 0 {
						if err := a.batchWriteItem(ctx, reqs, id); err != nil {
							ers = multierr.Append(ers, fmt.Errorf("a.batchWriteItem failed in processID %d, %w", id, err))
						}
					}
				case <-ctx.Done():
					ers = multierr.Append(ers, fmt.Errorf("ctx canceled in a.batch(): %w", ctx.Err()))
				}
			}
		}(ctx, i, batchData)
	}
	wg.Wait()
	writeDone <- ers
}

func (a App) batchWriteItem(ctx context.Context, reqs []types.WriteRequest, processID int) error {
	log.Printf("[DEBUG] processing a request (processID: %d, length: %d)\n", processID, len(reqs))
	const (
		backOffBase = 100
		maxRetries  = 8
	)
	var retries int
	rand.Seed(time.Now().UnixNano())
	for {
		out, err := a.ddbClient.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				*a.tableName: reqs,
			},
		})
		if err == nil {
			if len(out.UnprocessedItems) == 0 {
				log.Printf("[DEBUG] BatchWriteItem succeeded (processID: %d, data length: %d)", processID, len(reqs))
				break
			}
			reqs = out.UnprocessedItems[*a.tableName]
		}

		var throughputExceeded *types.ProvisionedThroughputExceededException
		if !errors.As(err, &throughputExceeded) {
			return fmt.Errorf("the API call of dynamodb:BatchWriteItem returned an error: %w", err)
		}

		if retries > maxRetries-2 {
			return fmt.Errorf("retry attempts reached the maximum value: %d", maxRetries)
		}
		duration := time.Duration(rand.Intn(backOffBase*(2<<retries))) * time.Millisecond
		log.Printf("[DEBUG] retry: an attempt (%d) after %v (processID: %d)\n", retries+2, duration, processID)
		time.Sleep(duration)
		retries++
	}

	return nil
}
