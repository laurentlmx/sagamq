package workflows

import (
    "testing"
    "time"
)

func TestNoRetryPolicy(t *testing.T) {
    var p NoRetryPolicy

    if p.Delay(1) != 0 {
        t.Fatalf("expected 0 delay, got %v", p.Delay(1))
    }
    if p.ShouldRetry(1, time.Second) {
        t.Fatalf("expected no retry")
    }
}

func TestExponentialBackoffPolicy(t *testing.T) {
    p := NewExponentialBackoffPolicy(100*time.Millisecond, 5*time.Second)

    d1 := p.Delay(1)
    d2 := p.Delay(2)
    d3 := p.Delay(3)

    if d1 != 100*time.Millisecond {
        t.Fatalf("expected 100ms, got %v", d1)
    }
    if d2 != 200*time.Millisecond {
        t.Fatalf("expected 200ms, got %v", d2)
    }
    if d3 != 400*time.Millisecond {
        t.Fatalf("expected 400ms, got %v", d3)
    }

    if !p.ShouldRetry(1, 1*time.Second) {
        t.Fatalf("expected retry allowed")
    }
    if p.ShouldRetry(1, 10*time.Second) {
        t.Fatalf("expected retry denied")
    }
}

func TestJitterRetryPolicy(t *testing.T) {
    p := NewJitterRetryPolicy(2, 100*time.Millisecond, 50*time.Millisecond)

    for i := 0; i < 10; i++ {
        d := p.Delay(1)
        if d < 100*time.Millisecond || d > 150*time.Millisecond {
            t.Fatalf("expected delay between 100ms and 150ms, got %v", d)
        }
    }

    if !p.ShouldRetry(1, 0) {
        t.Fatalf("expected retry allowed")
    }
    if !p.ShouldRetry(2, 0) {
        t.Fatalf("expected retry allowed")
    }
    if p.ShouldRetry(3, 0) {
	    t.Fatalf("expected retry denied after max allowed attempts")
    }
}
