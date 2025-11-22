package google

import (
	"cloud.google.com/go/pubsub/v2"
	"context"
	"encoding/base64"
	"fmt"
	"google.golang.org/api/option"
	"log"
	"time"
)

func ConnectToPubsub(ctx context.Context, authenticationKey string, projectID string) (*pubsub.Client, error) {

	jsonKey, err := base64.StdEncoding.DecodeString(authenticationKey)
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64 credentials: %w", err)
	}

	const maxAttempts = 10

	initialBackoff := time.Second

	backoff := initialBackoff

	for attempt := 1; attempt <= maxAttempts; attempt++ {

		client, err := pubsub.NewClient(ctx, projectID, option.WithCredentialsJSON(jsonKey))
		if err == nil {
			return client, nil
		}

		log.Printf("Attempt %d/%d: Failed to connect to Pub/Sub. Retrying in %v. Error: %v",
			attempt, maxAttempts, backoff, err)

		// If this was the last attempt, don't sleep
		if attempt == maxAttempts {
			break
		}

		select {
		case <-time.After(backoff):
			backoff *= 2
		case <-ctx.Done():
			// Context was cancelled, return the context error
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("failed to establish Pub/Sub connection after %d attempts: %w", maxAttempts, err)
}
