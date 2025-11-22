package abstract

import "context"

type SubscriptionHandler interface {
	Handle(ctx context.Context, body any) error
}

type SubscriptionHandlerFunc func(context.Context, any) error

func (f SubscriptionHandlerFunc) Handle(ctx context.Context, body any) error {
	return f(ctx, body)
}

type Subscriber interface {
	Listen(ctx context.Context, handler SubscriptionHandler)
	Close() error
}
