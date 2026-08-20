package mq

import (
    "errors"
    "testing"
    "time"

    "github.com/rabbitmq/amqp091-go"
)

// ---------------------------------------------------------------------------
// Test Doubles
// ---------------------------------------------------------------------------

type mockChannel struct {
    publishErr   error
    confirmErr   error
    confirmCh    chan amqp091.Confirmation
}

func (m *mockChannel) Publish(exchange, key string, mandatory, immediate bool, msg amqp091.Publishing) error {
    return m.publishErr
}

func (m *mockChannel) Close() error { return nil }

func (m *mockChannel) Confirm(noWait bool) error {
    return m.confirmErr
}

func (m *mockChannel) NotifyPublish(ch chan amqp091.Confirmation) chan amqp091.Confirmation {
    m.confirmCh = ch
    return ch
}

// ---------------------------------------------------------------------------
// Tests for NewSender()
// ---------------------------------------------------------------------------

func TestNew_EmptyEndpoint(t *testing.T) {
    _, err := NewSender("", "queue", time.Second)
    if err == nil {
        t.Fatal("expected error for empty endpoint")
    }
}

func TestNew_EmptyQueue(t *testing.T) {
    _, err := NewSender("amqp://localhost", "", time.Second)
    if err == nil {
        t.Fatal("expected error for empty queue")
    }
}

func TestNew_InvalidConfirmTimeout(t *testing.T) {
    _, err := NewSender("amqp://localhost", "queue", 0)
    if err == nil {
        t.Fatal("expected error for invalid confirm timeout")
    }
}

func TestNew_ConnectionFailure(t *testing.T) {
    _, err := NewSender("amqp://127.0.0.1:1", "queue", time.Second)
    if err == nil {
        t.Fatal("expected connection failure")
    }
}

// ---------------------------------------------------------------------------
// Tests for Publish()
// ---------------------------------------------------------------------------

func TestPublish_EmptyPayload(t *testing.T) {
    s := &Sender{
        channel: &mockChannel{},
    }

    err := s.Publish(false, []byte{}, 0)
    if err == nil || err.Error() != "payload cannot be empty" {
        t.Fatalf("expected empty payload error, got %v", err)
    }
}

func TestPublish_NegativeDelay(t *testing.T) {
    s := &Sender{
        channel: &mockChannel{},
    }

    err := s.Publish(false, []byte("data"), -1)
    if err == nil || err.Error() != "delay cannot be negative" {
        t.Fatalf("expected negative delay error, got %v", err)
    }
}

func TestPublish_NoChannel(t *testing.T) {
    s := &Sender{
        channel: nil,
    }

    err := s.Publish(false, []byte("data"), 0)
    if err == nil || err.Error() != "broker channel is not initialized" {
        t.Fatalf("expected channel error, got %v", err)
    }
}

func TestPublish_PublishFailure(t *testing.T) {
    mock := &mockChannel{
        publishErr: errors.New("publish failed"),
    }

    s := &Sender{
        channel: mock,
        confirms: make(chan amqp091.Confirmation, 1),
    }

    err := s.Publish(false, []byte("data"), 0)
    if err == nil || err.Error() != "publish failed: publish failed" {
        t.Fatalf("expected publish failure, got %v", err)
    }
}

func TestPublish_ConfirmAck(t *testing.T) {
    mock := &mockChannel{}
    confirmCh := make(chan amqp091.Confirmation, 1)
    confirmCh <- amqp091.Confirmation{Ack: true}

    s := &Sender{
        channel:  mock,
        confirms: confirmCh,
        confirmTimeout: time.Second,
    }

    err := s.Publish(false, []byte("data"), 0)
    if err != nil {
        t.Fatalf("expected success, got %v", err)
    }
}

func TestPublish_ConfirmNack(t *testing.T) {
    mock := &mockChannel{}
    confirmCh := make(chan amqp091.Confirmation, 1)
    confirmCh <- amqp091.Confirmation{Ack: false}

    s := &Sender{
        channel:  mock,
        confirms: confirmCh,
        confirmTimeout: time.Second,
    }

    err := s.Publish(false, []byte("data"), 0)
    if err == nil || err.Error() != "message was nacked by broker" {
        t.Fatalf("expected nack error, got %v", err)
    }
}

func TestPublish_ConfirmTimeout(t *testing.T) {
    mock := &mockChannel{}
    confirmCh := make(chan amqp091.Confirmation) // no ack sent

    s := &Sender{
        channel:  mock,
        confirms: confirmCh,
        confirmTimeout: 50 * time.Millisecond,
    }

    err := s.Publish(false, []byte("data"), 0)
    if err == nil || err.Error() != "publish confirmation timeout after 50ms" {
        t.Fatalf("expected timeout error, got %v", err)
    }
}
