package google

import (
	"cloud.google.com/go/pubsub/v2"
	"context"
	"encoding/base64"
	"google.golang.org/api/option"
)

func EnsureConnection(ctx context.Context, authenticationKey string, projectID string) (*pubsub.Client, error) {
	jsonKey, err := base64.StdEncoding.DecodeString(authenticationKey)
	if err != nil {
		return nil, err
	}
	client, err := pubsub.NewClient(ctx, projectID, option.WithCredentialsJSON(jsonKey))
	if err != nil {
		return nil, err
	}

	return client, nil
}
