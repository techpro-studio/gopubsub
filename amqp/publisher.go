package amqp

import (
	"context"
	"encoding/json"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
	"time"
)

type Publisher struct {
	url         string
	exchange    string
	mu          sync.Mutex
	conn        *amqp.Connection
	channel     *amqp.Channel
	confirmChan chan amqp.Confirmation
	closeChan   chan *amqp.Error
	jobs        chan publishJob
	onceInit    sync.Once
}

type publishJob struct {
	routingKey string
	body       []byte
}

func NewPublisher(url, exchange string) *Publisher {
	p := &Publisher{url: url, exchange: exchange}
	p.jobs = make(chan publishJob, 10000)
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
	go func() {
		for job := range p.jobs {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			_ = p.publishOnce(ctx, job.routingKey, job.body)
			cancel()
		}
	}()
}

func (p *Publisher) Publish(
	ctx context.Context,
	routingKey string,
	payload any,
) error {
	p.onceInit.Do(func() { p.startWorker() })
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	select {
	case p.jobs <- publishJob{routingKey: routingKey, body: body}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Publisher) Close() error {
	close(p.jobs)
	if p.channel != nil && !p.channel.IsClosed() {
		_ = p.channel.Close()
	}
	return p.conn.Close()
}

func (p *Publisher) forceReconnect() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.conn != nil && !p.conn.IsClosed() {
		_ = p.conn.Close() // ignore close error; we’re discarding it anyway
	}
	if p.channel != nil && !p.channel.IsClosed() {
		_ = p.channel.Close()
	}
	p.conn = nil // next ensureConn() will dial again
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
	case <-p.confirmChan:
		return nil
	case <-p.closeChan:
		return fmt.Errorf("channel closed by broker")
	case <-ctx.Done():
		return ctx.Err()
	}
}
