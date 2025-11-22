package google

import (
	"cloud.google.com/go/pubsub/v2"
	"context"
	"github.com/go-jose/go-jose/v4/json"
	"github.com/techpro-studio/gopubsub/abstract"
	"log"
)

type DefaultSubscriber struct {
	authenticationKey string
	projectID         string
	topic             string
	client            *pubsub.Client
	subscriber        *pubsub.Subscriber
}

func NewDefaultSubscriber(ctx context.Context, authenticationKey string, projectID string, subscriptionID string) (*DefaultSubscriber, error) {
	client, err := ConnectToPubsub(ctx, authenticationKey, projectID)
	if err != nil {
		return nil, err
	}

	sub := client.Subscriber(subscriptionID)

	return &DefaultSubscriber{
		authenticationKey: authenticationKey,
		projectID:         projectID,
		topic:             subscriptionID,
		subscriber:        sub,
		client:            client,
	}, nil
}

func (s *DefaultSubscriber) Close() error {
	return s.client.Close()
}

func (s *DefaultSubscriber) Listen(ctx context.Context, handler abstract.SubscriptionHandler) {
	err := s.subscriber.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		var data any
		err := json.Unmarshal(m.Data, &data)
		if err != nil {
			log.Printf("Error unmarshalling pubsub message: %v", err)
			m.Ack()
			return
		}
		err = handler.Handle(ctx, data)
		if err != nil {
			m.Nack()
		} else {
			m.Ack()
		}
	})
	if err != nil {
		s.logError(err.Error(), "DefaultSubscriber receive")
	}

}

func (s *DefaultSubscriber) logError(err string, place string) {
	log.Printf("Error has been occured in place : %s , in abstract:  %s, error: %s",
		place, s.topic, err)
}
