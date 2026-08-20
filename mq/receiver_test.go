package mq

import (
    "context"
    "errors"
    "testing"
    "time"

    "github.com/rabbitmq/amqp091-go"
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

func TestReceiverConsume_NilHandler(t *testing.T) {
    r := &Receiver{}
    err := r.Consume(nil, nil)
    if err == nil || err.Error() != "handler cannot be nil" {
        t.Fatalf("expected handler error, got %v", err)
    }
}

func TestReceiverConsume_NilChannel(t *testing.T) {
    r := &Receiver{}
    err := r.Consume(nil, func(context.Context, []byte, any) error { return nil })
    if err == nil || err.Error() != "consumer channel is not initialized" {
        t.Fatalf("expected channel error, got %v", err)
    }
}

func TestReceiverConsume_QosFailure(t *testing.T) {
    fake := &FakeConsumerChannel{
        QosErr: errors.New("qos failed"),
    }

    r := &Receiver{channel: fake}

    err := r.Consume(nil, func(context.Context, []byte, any) error { return nil })
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

    err := r.Consume(nil, func(context.Context, []byte, any) error { return nil })
    if err == nil || err.Error() != "failed to start consuming: consume failed" {
        t.Fatalf("unexpected error: %v", err)
    }
}

func TestReceiverConsume_HandlerCallAndAck(t *testing.T) {
    fake := &FakeConsumerChannel{
        Deliveries: make(chan amqp091.Delivery, 1),
    }

    r := &Receiver{
        queue:   "q",
        channel: fake,
    }

    err := r.Consume(nil, func(ctx context.Context, body []byte, replay any) error {
        if string(body) != "hello" {
            t.Fatalf("unexpected body: %s", string(body))
        }
        if replay.(int32) != 42 {
            t.Fatalf("unexpected replay header: %v", replay)
        }
        return nil
    })
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }

    fa := &FakeAcknowledger{}
    fake.Deliveries <- amqp091.Delivery{
        Body: []byte("hello"),
        Headers: amqp091.Table{
            "x-replay": int32(42),
        },
	Acknowledger: fa,
    }

    time.Sleep(10 * time.Millisecond)

    if !fa.AckCalled {
        t.Fatalf("expected Ack to be called")
    }
}

func TestReceiverConsume_HandlerErrorAndNack(t *testing.T) {
    fake := &FakeConsumerChannel{
        Deliveries: make(chan amqp091.Delivery, 1),
    }

    r := &Receiver{
        queue:   "q",
        channel: fake,
    }

    err := r.Consume(nil, func(ctx context.Context, body []byte, replay any) error {
        return errors.New("Handler failure")
    })
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }

    fa := &FakeAcknowledger{}
    fake.Deliveries <- amqp091.Delivery{
        Body: []byte("hello"),
        Headers: amqp091.Table{
            "x-replay": int32(42),
        },
	Acknowledger: fa,
    }

    time.Sleep(10 * time.Millisecond)

    if !fa.NackCalled {
        t.Fatalf("expected Nack to be called")
    }
}
