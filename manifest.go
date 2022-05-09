package importer

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/service/s3"
)

type Summary struct {
	ManifestFilesS3Key *string `json:"manifestFilesS3Key"`
	OutputFormat       *string `json:"outputFormat"`
}

func loadSummary(ctx context.Context, s3Client *s3.S3, bucket *string, key *string) (*Summary, error) {
	var summary Summary
	output, err := s3Client.GetObjectWithContext(ctx, &s3.GetObjectInput{Bucket: bucket, Key: key})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch the summary file: %w", err)
	}
	if err := json.NewDecoder(output.Body).Decode(&summary); err != nil {
		return nil, fmt.Errorf("failed to decode JSON in the summary file: %w", err)
	}

	// TODO: enhance the validation
	if *summary.ManifestFilesS3Key == "" || *summary.OutputFormat != "DYNAMODB_JSON" {
		return nil, fmt.Errorf("the summary file is invalid: %v", summary)
	}

	return &summary, nil
}

type Manifest struct {
	ItemCount     *int    `json:"itemCount"`
	DataFileS3Key *string `json:"dataFileS3Key"`
}

func loadManifests(ctx context.Context, s3Client *s3.S3, bucket *string, key *string) ([]Manifest, error) {
	output, err := s3Client.GetObjectWithContext(ctx, &s3.GetObjectInput{Bucket: bucket, Key: key})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch the manifest summary file: %w", err)
	}

	manifests := make([]Manifest, 0)
	scanner := bufio.NewScanner(output.Body)
	for scanner.Scan() {
		var manifest Manifest
		if err := json.Unmarshal(scanner.Bytes(), &manifest); err != nil {
			return nil, fmt.Errorf("failed to decode JSON in the manifest file: %w", err)
		}
		manifests = append(manifests, manifest)
	}
	return manifests, nil
}
