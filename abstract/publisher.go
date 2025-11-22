package abstract

import (
	"context"
	"time"
)

type Publisher interface {
	Publish(
		ctx context.Context,
		routingKey string,
		payload any,
		retry int,
		delay time.Duration,
	) error
	Close() error
}
