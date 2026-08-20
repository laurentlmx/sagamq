package mq

import (
    "errors"
    "fmt"
    "math"
    "time"

    "github.com/rabbitmq/amqp091-go"
)

const delayedExchange = "delayed.exchange"

type PublisherChannel interface {
    Publish(exchange, key string, mandatory, immediate bool, msg amqp091.Publishing) error
    Confirm(noWait bool) error
    NotifyPublish(confirm chan amqp091.Confirmation) chan amqp091.Confirmation
    Close() error
}

type Sender struct {
    endpoint       string
    queue          string
    confirmTimeout time.Duration

    conn     *amqp091.Connection
    channel  PublisherChannel
    confirms chan amqp091.Confirmation
}

func NewSender(endpoint, queue string, confirmTimeout time.Duration) (*Sender, error) {
    if endpoint == "" {
        return nil, fmt.Errorf("endpoint cannot be empty")
    }
    if queue == "" {
        return nil, fmt.Errorf("queue name cannot be empty")
    }
    if confirmTimeout <= 0 {
        return nil, fmt.Errorf("confirm timeout must be > 0")
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

    // Declare queue
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

    // Declare delayed exchange
    err = ch.ExchangeDeclare(
        delayedExchange,
        "x-delayed-message",
        true,
        false,
        false,
        false,
        amqp091.Table{
            "x-delayed-type": "direct",
        },
    )
    if err != nil {
        conn.Close()
        return nil, fmt.Errorf("failed to declare delayed exchange: %w", err)
    }

    // Bind queue to delayed exchange
    err = ch.QueueBind(
        queue,
        queue,            // routing key = queue name
        delayedExchange,
        false,
        nil,
    )
    if err != nil {
        conn.Close()
        return nil, fmt.Errorf("failed to bind queue: %w", err)
    }

    // Enable publisher confirms
    if err := ch.Confirm(false); err != nil {
        conn.Close()
        return nil, fmt.Errorf("failed to enable confirm mode: %w", err)
    }

    confirms := ch.NotifyPublish(make(chan amqp091.Confirmation, 1))

    return &Sender{
        endpoint:       endpoint,
        queue:          queue,
        confirmTimeout: confirmTimeout,
        conn:           conn,
        channel:        ch,
        confirms:       confirms,
    }, nil
}

// Publish sends a message with an optional delay.
// delay = 0 → immediate delivery
// delay > 0 → delayed delivery via LavinMQ
// It also specifies whether the message sent is being replayed so that the consuming workflow will know about it
func (s *Sender) Publish(replay bool, payload []byte, delay time.Duration) error {
    if len(payload) == 0 {
        return errors.New("payload cannot be empty")
    }
    if delay < 0 {
        return errors.New("delay cannot be negative")
    }
    if s.channel == nil {
        return errors.New("broker channel is not initialized")
    }

    ms := delay.Milliseconds()
    if ms > math.MaxInt32 {
        ms = math.MaxInt32
    }

    headers := amqp091.Table{
        "x-delay": int32(ms), // LavinMQ delayed exchange reads only 32‑bit integers for the delay value
	"x-replay": bool(replay),
    }

    err := s.channel.Publish(
        delayedExchange,
        s.queue, // routing key always equals queue name
        false,
        false,
        amqp091.Publishing{
            ContentType: "application/octet-stream",
            Body:        payload,
            Headers:     headers,
        },
    )
    if err != nil {
        return fmt.Errorf("publish failed: %w", err)
    }

    // Wait for broker confirmation
    select {
    case confirm := <-s.confirms:
        if !confirm.Ack {
            return fmt.Errorf("message was nacked by broker")
        }
        return nil

    case <-time.After(s.confirmTimeout):
        return fmt.Errorf("publish confirmation timeout after %s", s.confirmTimeout)
    }
}

func (s *Sender) Close() error {
    if s.channel != nil {
        s.channel.Close()
    }
    if s.conn != nil {
        return s.conn.Close()
    }
    return nil
}
