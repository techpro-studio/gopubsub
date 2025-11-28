package google

import (
	"cloud.google.com/go/pubsub/v2"
	"context"
	"github.com/go-jose/go-jose/v4/json"
	"github.com/techpro-studio/gopubsub/abstract"
	"sync"
)

type Publisher struct {
	authenticationKey string
	projectID         string
	client            *pubsub.Client
	mu                sync.RWMutex
	topicMap          map[string]*pubsub.Publisher
}

func NewPublisher(ctx context.Context, keyBase64 string, projectId string) (*Publisher, error) {
	publisher := &Publisher{
		authenticationKey: keyBase64,
		projectID:         projectId,
		topicMap:          make(map[string]*pubsub.Publisher), // Initialize the map!
	}

	client, err := ConnectToPubsub(ctx, keyBase64, projectId)
	if err != nil {
		return nil, err
	}
	publisher.client = client
	return publisher, nil
}

func (p *Publisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, publisher := range p.topicMap {
		publisher.Stop()
	}

	if p.client != nil {
		return p.client.Close()
	}
	return nil
}

func (p *Publisher) Publish(ctx context.Context, routingKey string, payload any) error {
	data, err := json.Marshal(&payload)
	if err != nil {
		return err
	}

	publisher := p.getTopic(routingKey)

	_ = publisher.Publish(ctx, &pubsub.Message{
		Data: data,
	})
	return nil
}

type PubsubResultWrapper struct {
	result *pubsub.PublishResult
}

func (p PubsubResultWrapper) Get(ctx context.Context) (any, error) {
	return p.result.Get(ctx)
}

func (p *Publisher) PublishAsync(
	ctx context.Context,
	routingKey string,
	payload any,
) abstract.AsyncPublisherResult {
	data, err := json.Marshal(&payload)
	if err != nil {
		panic(err)
	}

	publisher := p.getTopic(routingKey)

	result := publisher.Publish(ctx, &pubsub.Message{
		Data: data,
	})

	return PubsubResultWrapper{result: result}
}

func (p *Publisher) getTopic(routingKey string) *pubsub.Publisher {
	p.mu.RLock()
	publisher, ok := p.topicMap[routingKey]
	p.mu.RUnlock()

	if !ok {
		p.mu.Lock()
		publisher, ok = p.topicMap[routingKey]
		if !ok {
			publisher = p.client.Publisher(routingKey)

			p.topicMap[routingKey] = publisher
		}
		p.mu.Unlock()
	}
	return publisher
}
