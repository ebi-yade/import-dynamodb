package importer

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type DDB struct {
	hashKey string
}

func (a App) describeDDB(ctx context.Context) (DDB, error) {
	var (
		tableOut *dynamodb.DescribeTableOutput
		err      error
		res      DDB
		retries  int
	)
	maxRetries := 8 // wait for DynamoDB table status to be active at most 256 sec
	for {
		tableOut, err = a.ddbClient.DescribeTable(ctx, &dynamodb.DescribeTableInput{
			TableName: a.tableName,
		})
		if err != nil {
			return res, fmt.Errorf("failed to find the DynamoDB table: %s: %w", *a.tableName, err)
		}
		if tableOut.Table.TableStatus == "ACTIVE" {
			break
		}
		if retries > maxRetries-2 {
			return res, fmt.Errorf("the table status did not get ACTIVE after %d times attempts", maxRetries)
		}
		duration := 2 << retries
		log.Printf("[INFO] table exists, but the status is %s, so retry after %d sec\n", tableOut.Table.TableStatus, duration)
		time.Sleep(time.Duration(duration) * time.Second)
		retries++
	}

	var hashKey string
	for _, attr := range tableOut.Table.KeySchema {
		if attr.KeyType == "HASH" {
			hashKey = *attr.AttributeName
			break
		}
	}
	if hashKey == "" {
		return res, fmt.Errorf("failed to get hash key of the table: %s", *a.tableName)
	}

	res.hashKey = hashKey
	return res, nil
}
