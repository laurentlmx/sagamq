package mq

import (
    "context"
    "fmt"

    "github.com/rabbitmq/amqp091-go"
)

type ConsumerChannel interface {
    Qos(prefetchCount, prefetchSize int, global bool) error
    Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp091.Table) (<-chan amqp091.Delivery, error)
    Close() error
}

type Receiver struct {
    endpoint string
    queue    string

    conn    *amqp091.Connection
    channel ConsumerChannel
}

func NewReceiver(endpoint, queue string) (*Receiver, error) {
    if endpoint == "" {
        return nil, fmt.Errorf("endpoint cannot be empty")
    }
    if queue == "" {
        return nil, fmt.Errorf("queue name cannot be empty")
    }

    conn, err := amqp091.Dial(endpoint)
    if err != nil {
        return nil, fmt.Errorf("failed to connect to broker: %w", err)
    }

    ch, err := conn.Channel()
    if err != nil {
        conn.Close()
        return nil, fmt.Errorf("failed to open channel: %w", err)
    }

    // Ensure queue exists (idempotent)
    _, err = ch.QueueDeclare(
        queue,
        true,  // durable
        false, // auto-delete
        false, // exclusive
        false, // no-wait
        nil,
    )
    if err != nil {
        conn.Close()
        return nil, fmt.Errorf("failed to declare queue: %w", err)
    }

    return &Receiver{
        endpoint: endpoint,
        queue:    queue,
        conn:     conn,
        channel:  ch, // ch implements ConsumerChannel
    }, nil
}

func (r *Receiver) Consume(ctx context.Context, handler func(context.Context, []byte, any) error) error {
    if handler == nil {
        return fmt.Errorf("handler cannot be nil")
    }
    if r.channel == nil {
        return fmt.Errorf("consumer channel is not initialized")
    }

    // Ensure we process one message at a time
    if err := r.channel.Qos(1, 0, false); err != nil {
        return fmt.Errorf("failed to set QoS: %w", err)
    }

    deliveries, err := r.channel.Consume(
        r.queue,
        "",
        false, // manual ACK
        false, // exclusive
        false, // no-local
        false, // no-wait
        nil,   // args
    )
    if err != nil {
        return fmt.Errorf("failed to start consuming: %w", err)
    }

    go func() {
        for msg := range deliveries { // Keeps listening for new messages as long as the receiver is running
            // Extract x-replay header if present
            var replay any
            if msg.Headers != nil {
                if v, ok := msg.Headers["x-replay"]; ok {
                    replay = v
                }
            }

            // Pass both body and replay value to handler
            if err := handler(ctx, msg.Body, replay); err != nil {
                _ = msg.Nack(false, false) // Assuming messages are sent to DLX rather than dropped...
                continue
            }
	    _ = msg.Ack(false)
        }
    }()

    return nil
}

func (r *Receiver) Close() error {
    if r.channel != nil {
        _ = r.channel.Close()
    }
    if r.conn != nil {
        return r.conn.Close()
    }
    return nil
}
