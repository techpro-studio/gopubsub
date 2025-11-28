package abstract

import (
	"context"
)

type PublishResult interface {
	Get(ctx context.Context) (any, error)
}

type Publisher interface {
	Publish(
		ctx context.Context,
		routingKey string,
		payload any,
	) PublishResult
	Close() error
}
