package google

import (
	"cloud.google.com/go/pubsub/v2"
	"context"
	"github.com/go-jose/go-jose/v4/json"
	"github.com/techpro-studio/gopubsub/abstract"
	"log"
	"time"
)

type DefaultSubscriber struct {
	authenticationKey string
	projectID         string
	topic             string
	client            *pubsub.Client
}

func NewDefaultSubscriber(authenticationKey string, projectID string, topic string) *DefaultSubscriber {
	return &DefaultSubscriber{authenticationKey: authenticationKey, projectID: projectID, topic: topic}
}

func (s *DefaultSubscriber) Listen(ctx context.Context, handler abstract.SubscriptionHandler) {
	backoff := time.Second

	for {
		if s.client == nil {
			client, err := EnsureConnection(ctx, s.authenticationKey, s.projectID)
			if err != nil {
				time.Sleep(backoff)
				backoff *= 2
				s.logError(err.Error(), "Client create")
				continue
			} else {
				s.client = client
				break
			}
		}
	}

	err := s.client.Subscriber(s.topic).Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		var data any
		err := json.Unmarshal(m.Data, &data)
		if err != nil {
			m.Nack()
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
