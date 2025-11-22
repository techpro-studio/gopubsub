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
	url      string
	exchange string
	mu       sync.Mutex
	conn     *amqp.Connection
}

func NewPublisher(url, exchange string) *Publisher {
	return &Publisher{url: url, exchange: exchange}
}

// ensureConn lazily dials or returns the existing open connection.
func (p *Publisher) ensureConn() (*amqp.Connection, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.conn != nil && !p.conn.IsClosed() {
		return p.conn, nil
	}

	conn, err := amqp.DialConfig(p.url, amqp.Config{
		Heartbeat: 10 * time.Second,
		Locale:    "en_US",
	})
	if err != nil {
		return nil, err
	}
	p.conn = conn
	return conn, nil
}

func (p *Publisher) Publish(
	ctx context.Context,
	routingKey string,
	payload any,
	retry int,
	delay time.Duration,
) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	var attempt int
	for {
		attempt++
		err = p.publishOnce(ctx, routingKey, body)
		if err == nil || attempt > retry {
			return err
		}
		time.Sleep(delay)
	}
}

func (p *Publisher) Close() error {
	return p.conn.Close()
}

func (p *Publisher) forceReconnect() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.conn != nil && !p.conn.IsClosed() {
		_ = p.conn.Close() // ignore close error; we’re discarding it anyway
	}
	p.conn = nil // next ensureConn() will dial again
}

func (p *Publisher) publishOnce(ctx context.Context, key string, body []byte) error {
	conn, err := p.ensureConn()
	if err != nil {
		return err
	}

	ch, err := conn.Channel()
	if err != nil {
		p.forceReconnect()
		return err
	}
	defer ch.Close()

	// Track broker-initiated closes
	closeErr := ch.NotifyClose(make(chan *amqp.Error, 1))

	if err := ch.Confirm(false); err != nil {
		return err
	}
	await := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	if err := ch.Publish(p.exchange, key, true, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		Body:         body,
	}); err != nil {
		return err
	}

	select {
	case c := <-await:
		if !c.Ack {
			return fmt.Errorf("broker nack’d the message")
		}
		return nil
	case e := <-closeErr:
		return fmt.Errorf("channel closed by broker: %v", e)
	case <-ctx.Done():
		return ctx.Err()
	}
}
