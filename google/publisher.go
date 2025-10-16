package google

import (
	"cloud.google.com/go/pubsub/v2"
	"context"
	"github.com/go-jose/go-jose/v4/json"
	"time"
)

type Publisher struct {
	authenticationKey string
	projectID         string
	client            *pubsub.Client
}

func NewPublisher(keyBase64 string, projectId string) *Publisher {
	return &Publisher{authenticationKey: keyBase64, projectID: projectId}
}

func (p *Publisher) ensureConnection() error {
	client, err := EnsureConnection(context.Background(), p.authenticationKey, p.projectID)
	if err != nil {
		return err
	}
	p.client = client
	return nil
}

func (p *Publisher) Publish(ctx context.Context, routingKey string, payload any, retry int, delay time.Duration) error {
	var attempt int
	for {
		attempt++
		err := p.publishOnce(ctx, routingKey, payload)
		if err == nil || attempt > retry {
			return err
		}
		time.Sleep(delay)
	}
}

func (p *Publisher) publishOnce(ctx context.Context, routingKey string, payload any) error {
	err := p.ensureConnection()
	if err != nil {
		return err
	}

	data, err := json.Marshal(&payload)
	if err != nil {
		return err
	}

	result := p.client.Publisher(routingKey).Publish(ctx, &pubsub.Message{
		Data: data,
	})
	_, err = result.Get(ctx)
	return err
}
