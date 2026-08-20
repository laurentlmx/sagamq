package workflows

import (
    "math"
    "math/rand"
    "time"
)

type RetryPolicy interface {
    Delay(attempt int) time.Duration
    ShouldRetry(attempt int, elapsed time.Duration) bool
}

//
// NoRetryPolicy
//

type NoRetryPolicy struct{}

func (NoRetryPolicy) Delay(int) time.Duration { return 0 }
func (NoRetryPolicy) ShouldRetry(int, time.Duration) bool { return false }

//
// ExponentialBackoffPolicy
//

type ExponentialBackoffPolicy struct {
    Initial time.Duration
    Max     time.Duration
}

func NewExponentialBackoffPolicy(initial, max time.Duration) *ExponentialBackoffPolicy {
    return &ExponentialBackoffPolicy{Initial: initial, Max: max}
}

func (p *ExponentialBackoffPolicy) Delay(attempt int) time.Duration {
    if attempt < 1 {
        attempt = 1
    }
    factor := math.Pow(2, float64(attempt-1))
    return time.Duration(float64(p.Initial) * factor)
}

func (p *ExponentialBackoffPolicy) ShouldRetry(attempt int, elapsed time.Duration) bool {
    return elapsed < p.Max
}

//
// JitterRetryPolicy
//

type JitterRetryPolicy struct {
    MaxRetries int
    Base        time.Duration
    Jitter      time.Duration
    rnd         *rand.Rand
}

func NewJitterRetryPolicy(maxRetries int, base, jitter time.Duration) *JitterRetryPolicy {
    return &JitterRetryPolicy{
        MaxRetries:  maxRetries,
        Base:        base,
        Jitter:      jitter,
        rnd:         rand.New(rand.NewSource(time.Now().UnixNano())),
    }
}

func (p *JitterRetryPolicy) Delay(attempt int) time.Duration {
    if p.Jitter <= 0 {
        return p.Base
    }
    j := time.Duration(p.rnd.Int63n(int64(p.Jitter) + 1))
    return p.Base + j
}

func (p *JitterRetryPolicy) ShouldRetry(attempt int, _ time.Duration) bool {
    return attempt <= p.MaxRetries
}
