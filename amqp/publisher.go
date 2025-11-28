package amqp

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/techpro-studio/gopubsub/abstract"
)

type Publisher struct {
	url         string
	exchange    string
	mu          sync.Mutex
	conn        *amqp.Connection
	channel     *amqp.Channel
	confirmChan chan amqp.Confirmation
	closeChan   chan *amqp.Error
	jobs        chan *publishJob
	onceInit    sync.Once
	wg          sync.WaitGroup
}

type publishJob struct {
	routingKey string
	body       []byte
	result     *amqpPublishResult
}

// PublishResult represents an async publish operation (future pattern)
type amqpPublishResult struct {
	ready chan struct{}
	err   error
	once  sync.Once
}

func NewPublisher(url, exchange string) *Publisher {
	p := &Publisher{
		url:      url,
		exchange: exchange,
		jobs:     make(chan *publishJob, 10000),
	}
	return p
}

func (p *Publisher) ensureChan() (*amqp.Channel, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.conn == nil || p.conn.IsClosed() {
		conn, err := amqp.DialConfig(p.url, amqp.Config{
			Heartbeat: 10 * time.Second,
			Locale:    "en_US",
		})
		if err != nil {
			return nil, err
		}
		p.conn = conn
	}

	// Reuse existing channel if open
	if p.channel != nil && !p.channel.IsClosed() {
		return p.channel, nil
	}

	// Create new channel
	ch, err := p.conn.Channel()
	if err != nil {
		p.forceReconnect()
		return nil, err
	}

	// Enable confirm mode once per channel
	if err := ch.Confirm(false); err != nil {
		ch.Close()
		return nil, err
	}

	// Initialize shared confirmation + close channels once per channel
	p.confirmChan = ch.NotifyPublish(make(chan amqp.Confirmation, 10000))
	p.closeChan = ch.NotifyClose(make(chan *amqp.Error, 1))

	p.channel = ch
	return ch, nil
}

func (p *Publisher) startWorker() {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for job := range p.jobs {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			err := p.publishOnce(ctx, job.routingKey, job.body)
			job.result.complete(err)
			cancel()
		}
	}()
}

func (p *Publisher) Publish(
	ctx context.Context,
	routingKey string,
	payload any,
) abstract.PublishResult {
	p.onceInit.Do(func() { p.startWorker() })

	result := &amqpPublishResult{
		ready: make(chan struct{}),
	}

	body, err := json.Marshal(payload)
	if err != nil {
		result.complete(fmt.Errorf("marshal: %w", err))
		return result
	}

	job := &publishJob{
		routingKey: routingKey,
		body:       body,
		result:     result,
	}

	select {
	case p.jobs <- job:
		return result
	case <-ctx.Done():
		result.complete(ctx.Err())
		return result
	}
}

func (p *Publisher) Close() error {
	close(p.jobs)
	p.wg.Wait() // Wait for all publish jobs to complete

	if p.channel != nil && !p.channel.IsClosed() {
		_ = p.channel.Close()
	}
	if p.conn != nil && !p.conn.IsClosed() {
		return p.conn.Close()
	}
	return nil
}

func (p *Publisher) forceReconnect() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.conn != nil && !p.conn.IsClosed() {
		_ = p.conn.Close()
	}
	if p.channel != nil && !p.channel.IsClosed() {
		_ = p.channel.Close()
	}
	p.conn = nil
	p.channel = nil
}

func (p *Publisher) publishOnce(ctx context.Context, key string, body []byte) error {
	ch, err := p.ensureChan()
	if err != nil {
		return err
	}

	if err := ch.Publish(p.exchange, key, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		Body:         body,
	}); err != nil {
		return err
	}

	select {
	case conf := <-p.confirmChan:
		if !conf.Ack {
			return fmt.Errorf("publish not acknowledged by broker")
		}
		return nil
	case <-p.closeChan:
		return fmt.Errorf("channel closed by broker")
	case <-ctx.Done():
		return ctx.Err()
	}
}

// PublishResult implementation

func (r *amqpPublishResult) complete(err error) {
	r.once.Do(func() {
		r.err = err
		close(r.ready)
	})
}

func (r *amqpPublishResult) Get(ctx context.Context) (any, error) {
	select {
	case <-r.ready:
		return nil, r.err // AMQP doesn't provide message IDs like Pub/Sub
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (r *amqpPublishResult) Ready() <-chan struct{} {
	return r.ready
}
