package importer

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/ebi-yade/import-dynamodb/convert"
)

type row struct {
	Item map[string]events.DynamoDBAttributeValue `json:"Item"`
}

func (a App) importByManifest(ctx context.Context, ddbClient *dynamodb.Client, s3Client *s3.Client, manifest Manifest, ddb DDB) error {
	data, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
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

		// debug!!
		log.Printf("hash key: %#v", item["HashKey"])
		break

	}
	return nil
}
