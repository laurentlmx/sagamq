package mq

import (
    "context"
    "fmt"

    "github.com/rabbitmq/amqp091-go"
    "github.com/laurentlmx/sagamq/workflows"
)

type ConsumerChannel interface { // This interface declaration is for tests purpose
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

func (r *Receiver) Consume(ctx context.Context, wfCtx *workflows.Context, wfEngine *workflows.WorkflowEngine, wfStartTaskName string) error {
    if wfEngine == nil {
        return fmt.Errorf("workflow engine cannot be nil")
    }
    if wfStartTaskName == "" {
        return fmt.Errorf("start task name cannot be empty")
    }
    if r.channel == nil {
        return fmt.Errorf("consumer channel is not initialized")
    }
    if wfCtx == nil {
	wfCtx = workflows.NewContext()
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
	    replay := false
	    if v, ok := msg.Headers["x-replay"].(bool); ok {
    		replay = v
	    }

            // launch the workflow with both message body and replay value added to its context
	    wfCtx.Set("replay", replay)
	    wfCtx.Set(msg.MessageId, msg.Body)
            if err := wfEngine.Run(ctx, wfStartTaskName, wfCtx); err != nil {
                _ = msg.Nack(false, false) // Assuming messages are sent to DLX rather than dropped...
                continue
            }
	    _ = msg.Ack(true)
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
