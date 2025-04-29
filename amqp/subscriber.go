package amqp

import (
	"context"
	"encoding/json"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	pubsub "github.com/techpro-studio/gopubsub"
	"log"
	"time"
)

type Subscriber struct {
	url          string
	queue        string
	routingKey   string
	exchange     string
	needAck      bool
	listenerName string
}

func NewSubscriber(url string, name string, listenerName string, needAck bool, exchange string, routingKey string) *Subscriber {
	return &Subscriber{url: url, queue: name, listenerName: listenerName, needAck: needAck, exchange: exchange, routingKey: routingKey}
}

func (s *Subscriber) Listen(ctx context.Context, handler pubsub.SubscriptionHandler) {
	backoff := time.Second

	for {
		conn, err := amqp.Dial(s.url)
		if err != nil {
			s.logError(err.Error(), "dial")
			time.Sleep(backoff)
			backoff = min(backoff*2, 30*time.Second)
			continue
		}
		backoff = time.Second
		ch, err := conn.Channel()
		if err != nil {
			s.logError(err.Error(), "channel init")
			_ = conn.Close()
			continue
		}
		q, err := ch.QueueDeclare(
			s.queue, // queue queue, e.g. "email-service"
			true,    // durable
			false,   // autoDelete
			false,   // exclusive
			false,   // noWait
			nil,     // arguments
		)
		if err != nil {
			s.logError(err.Error(), "queue declare")
			_ = conn.Close()
			continue
		}

		// 1. bind it to amq.direct with the routing-Key you expect
		if err := ch.QueueBind(
			q.Name,
			s.routingKey, // ← routing-key that publisher uses
			s.exchange,   // exchange
			false,
			nil,
		); err != nil {
			s.logError(err.Error(), "queue bind")
			_ = conn.Close()
			continue
		}
		err = ch.Qos(1, 0, false)
		if err != nil {
			s.logError(err.Error(), "QOS")
			continue
		}

		msgs, err := ch.Consume(s.queue, s.listenerName, false, false, false, false, nil)
		if err != nil {
			s.logError(err.Error(), "consume")
			_ = ch.Close()
			_ = conn.Close()
			continue
		}

		errorNotify := ch.NotifyClose(make(chan *amqp.Error))
		cancelNotify := ch.NotifyCancel(make(chan string))

		// consumer loop
		go func() {
			for d := range msgs {
				if ctx.Err() != nil {
					return
				}
				var evt any
				if err := json.Unmarshal(d.Body, &evt); err != nil {
					s.logError(err.Error(), "json parse")
					if s.needAck {
						err := d.Nack(false, false)
						if err != nil {
							s.logError(err.Error(), "nack error json")
						} // send to DLQ
					}
					continue
				}

				func() {
					defer func() {
						if r := recover(); r != nil {
							s.logError(fmt.Sprint(r), "panic in handler")
						}
					}()
					if err := handler.Handle(ctx, evt); err != nil {
						s.logError(err.Error(), "handler")
						if s.needAck {
							err := d.Nack(false, true)
							if err != nil {
								s.logError(err.Error(), "nack")
							} // retry
						}
						return
					}
					if s.needAck {
						if err := d.Ack(false); err != nil {
							s.logError(err.Error(), "ack")
						}
					}
				}()
			}
		}()

		// wait for close/cancel/ctx
		select {
		case rcErr := <-errorNotify:
			s.logError(fmt.Sprint(rcErr), "connection closed")
		case tag := <-cancelNotify:
			s.logError("consumer cancelled: "+tag, "consumer")
		case <-ctx.Done():
			_ = ch.Cancel(s.listenerName, false)
			_ = conn.Close()
			return
		}

		_ = ch.Close()
		_ = conn.Close()
	}
}

func (s *Subscriber) logError(err string, place string) {
	log.Printf("Error has been occured in place : %s , in pubsub:  %s, with listener: %s, error: %s",
		place, s.queue, s.listenerName, err)
}
