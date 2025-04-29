package pubsub

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
}

type SubscriptionHandler interface {
	Handle(ctx context.Context, body any) error
}

type SubscriptionHandlerFunc func(context.Context, any) error

func (f SubscriptionHandlerFunc) Handle(ctx context.Context, body any) error {
	return f(ctx, body)
}

type Subscriber interface {
	Listen(ctx context.Context, handler SubscriptionHandler)
}
