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
