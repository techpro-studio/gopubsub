package google

import (
	"cloud.google.com/go/pubsub/v2"
	"context"
	"github.com/go-jose/go-jose/v4/json"
	"log"
	"sync"
	"time"
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

func (p *Publisher) publishOnce(ctx context.Context, routingKey string, payload any) error {
	data, err := json.Marshal(&payload)
	if err != nil {
		return err
	}

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

	result := publisher.Publish(ctx, &pubsub.Message{
		Data: data,
	})
	serverId, err := result.Get(ctx)
	if err != nil {
		return err
	}
	log.Printf("Published a message with routing key %s to %s", routingKey, serverId)
	return err
}
