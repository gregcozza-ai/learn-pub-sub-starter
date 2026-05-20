package pubsub

import (
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Acktype int 

const (
	Ack Acktype = iota
	NackRequeue
	NackDiscard
)

type SimpleQueueType int 

const (
	SimpleQueueDurable SimpleQueueType = iota 
	SimpleQueueTransient
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not create channel: %v", err) 
	}

	// Configure dead letter exchange
	args := amqp.Table {
		"x-dead-letter-exchange": "peril_dlx", // Name of dead letter exchange 
	}

	queue, err := ch.QueueDeclare(
		queueName,							// name
		queueType == SimpleQueueDurable,	// durable
		queueType != SimpleQueueDurable,	// delete when unused 
		queueType != SimpleQueueDurable,	// exclusive 
		false,								// no-wait
		args,								// args with dead letter config 
	)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not declare queue: %v", err) 
	}

	err = ch.QueueBind(
		queueName,	// queue name
		key,		// routing key
		exchange,	// exchange
		false,		// no-wait
		nil,		// args 
	)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not bind queue: %v", err )
	}

	return ch, queue, nil 
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) Acktype,
) error {
	ch, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("could not declare and bind queue: %v", err)
	}

	deliveries, err := ch.Consume(
		queueName,			// queue
		"",					// comsumer
		false,				// auto-ack
		false,				// exclusive
		false,				// no-local
		false,				// no-wait
		nil, 				// args 
	)
	if err != nil {
		return fmt.Errorf("could not consume messages: %v", err)
	}

	go func() {
		for d := range deliveries {
			var msg T 
			if err := json.Unmarshal(d.Body, &msg); err != nil {
				fmt.Printf("could not unmarshal message: %v\n", err)
				continue 
			}

			ackType := handler(msg)
			switch ackType {
			case Ack:
				fmt.Println("ACK: Message processed successfully")
				d.Ack(false)
			case NackRequeue:
				fmt.Println("NACK (requeue): Message needs reprocessing")
				d.Nack(false, true)
			case NackDiscard:
				fmt.Println("NACK (discard): Message discarded")
				d.Nack(false, false)
			default:
				fmt.Println("UNKNOWN ACK: Defaulting to ACK")
				d.Ack(false)
			}
		}
	}()

	return nil 
}