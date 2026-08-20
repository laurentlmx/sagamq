package mq

import (
    "context"
    "errors"
    "log/slog"
    "os"
    "testing"
    "time"

    "github.com/rabbitmq/amqp091-go"
    "github.com/laurentlmx/sagamq/workflows"
)

// -----------------------------------------------------------------------------
// Fake Acknowlodger and ConsumerChannel for Consume() tests
// -----------------------------------------------------------------------------

type FakeAcknowledger struct {
    AckCalled    bool
    NackCalled   bool
    RejectCalled bool
}

func (f *FakeAcknowledger) Ack(tag uint64, multiple bool) error {
    f.AckCalled = true
    return nil
}

func (f *FakeAcknowledger) Nack(tag uint64, multiple bool, requeue bool) error {
    f.NackCalled = true
    return nil
}

func (f *FakeAcknowledger) Reject(tag uint64, requeue bool) error {
    f.RejectCalled = true
    return nil
}

type FakeConsumerChannel struct {
    Deliveries chan amqp091.Delivery

    QosCalled     bool
    QosErr        error
    ConsumeCalled bool
    ConsumeErr    error
}

func (f *FakeConsumerChannel) Qos(prefetchCount, prefetchSize int, global bool) error {
    f.QosCalled = true
    return f.QosErr
}

func (f *FakeConsumerChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp091.Table) (<-chan amqp091.Delivery, error) {
    f.ConsumeCalled = true
    return f.Deliveries, f.ConsumeErr
}

func (f *FakeConsumerChannel) Close() error { return nil }

// -----------------------------------------------------------------------------
// Constructor tests
// -----------------------------------------------------------------------------

func TestNewReceiver_EmptyEndpoint(t *testing.T) {
    _, err := NewReceiver("", "queue")
    if err == nil || err.Error() != "endpoint cannot be empty" {
        t.Fatalf("expected endpoint error, got %v", err)
    }
}

func TestNewReceiver_EmptyQueue(t *testing.T) {
    _, err := NewReceiver("amqp://test", "")
    if err == nil || err.Error() != "queue name cannot be empty" {
        t.Fatalf("expected queue error, got %v", err)
    }
}

func TestNewReceiver_ConnectionFailure(t *testing.T) {
    _, err := NewReceiver("amqp://bad", "queue")
    if err == nil {
        t.Fatalf("expected connection failure")
    }
}

// -----------------------------------------------------------------------------
// Consume() tests
// -----------------------------------------------------------------------------

var testLogger = slog.New(slog.NewTextHandler(os.Stdout, nil))

func TestReceiverConsume_NilWorkflowEngine(t *testing.T) {
    r := &Receiver{}
    err := r.Consume(nil, nil, nil, "")
    if err == nil || err.Error() != "workflow engine cannot be nil" {
        t.Fatalf("expected workflow engine error, got %v", err)
    }
}

func TestReceiverConsume_NoStartTask(t *testing.T) {
    r := &Receiver{}
    engine, _ := workflows.NewWorkflowEngine("", []workflows.Task{}, nil, testLogger)

    err := r.Consume(nil, nil, engine, "")
    if err == nil || err.Error() != "start task name cannot be empty" {
        t.Fatalf("expected start task error, got %v", err)
    }
}

func TestReceiverConsume_NilChannel(t *testing.T) {
    r := &Receiver{}
    engine, _ := workflows.NewWorkflowEngine("", []workflows.Task{}, nil, testLogger)

    err := r.Consume(nil, nil, engine, "Task1")
    if err == nil || err.Error() != "consumer channel is not initialized" {
        t.Fatalf("expected channel error, got %v", err)
    }
}

func TestReceiverConsume_QosFailure(t *testing.T) {
    fake := &FakeConsumerChannel{
        QosErr: errors.New("qos failed"),
    }

    r := &Receiver{channel: fake}
    engine, _ := workflows.NewWorkflowEngine("", []workflows.Task{}, nil, testLogger)

    err := r.Consume(nil, nil, engine, "Task1")
    if err == nil || err.Error() != "failed to set QoS: qos failed" {
        t.Fatalf("unexpected error: %v", err)
    }
}

func TestReceiverConsume_ConsumeFailure(t *testing.T) {
    fake := &FakeConsumerChannel{
        QosErr:     nil,
        ConsumeErr: errors.New("consume failed"),
    }

    r := &Receiver{channel: fake}

    engine, _ := workflows.NewWorkflowEngine("", []workflows.Task{}, nil, testLogger)

    err := r.Consume(nil, nil, engine, "Task1")
    if err == nil || err.Error() != "failed to start consuming: consume failed" {
        t.Fatalf("unexpected error: %v", err)
    }
}

func TestReceiverConsume_WorkflowExecutionAndAck(t *testing.T) {
    fake := &FakeConsumerChannel{
        Deliveries: make(chan amqp091.Delivery, 1),
    }

    r := &Receiver{
        queue:   "q",
        channel: fake,
    }

    task, _ := workflows.NewTask("Task1", func(ctx context.Context, c *workflows.Context, logger  *slog.Logger) error {
        return nil
    }, nil)

    engine, _ := workflows.NewWorkflowEngine("", []workflows.Task{task}, nil, testLogger)

    err := r.Consume(context.Background(), nil, engine, "Task1")
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }

    fa := &FakeAcknowledger{}
    fake.Deliveries <- amqp091.Delivery{
	MessageId: "My message",
        Body: []byte("some message payload"),
        Headers: amqp091.Table{
            "x-replay": bool(false),
        },
	Acknowledger: fa,
    }

    time.Sleep(10 * time.Millisecond)

    if !fa.AckCalled {
        t.Fatalf("expected Ack to be called")
    }
}

func TestReceiverConsume_WorkflowErrorAndNack(t *testing.T) {
    fake := &FakeConsumerChannel{
        Deliveries: make(chan amqp091.Delivery, 1),
    }

    r := &Receiver{
        queue:   "q",
        channel: fake,
    }

    task, _ := workflows.NewTask("FailingTask", func(ctx context.Context, c *workflows.Context, logger  *slog.Logger) error {
        return errors.New("Task failed")
    }, nil)

    engine, _ := workflows.NewWorkflowEngine("", []workflows.Task{task}, nil, testLogger)

    err := r.Consume(context.Background(), nil, engine, "FailingTask")
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }

    fa := &FakeAcknowledger{}
    fake.Deliveries <- amqp091.Delivery{
        MessageId: "My message",
        Body: []byte("some message payload"),
        Headers: amqp091.Table{
            "x-replay": bool(false),
         },
	Acknowledger: fa,
    }

    time.Sleep(10 * time.Millisecond)

    if !fa.NackCalled {
        t.Fatalf("expected Nack to be called")
    }
}
