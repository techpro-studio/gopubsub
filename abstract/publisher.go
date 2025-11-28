package abstract

import (
	"context"
)

type Publisher interface {
	Publish(
		ctx context.Context,
		routingKey string,
		payload any,
	) error
	Close() error
}

type AsyncPublisherResult interface {
	Get(ctx context.Context) (any, error)
}

type AsyncPublisher interface {
	PublishAsync(
		ctx context.Context,
		routingKey string,
		payload any,
	) AsyncPublisherResult
	Close() error
}
