package workflows

import (
    "fmt"
    "sync"
    "testing"
)

func TestContextSetGet(t *testing.T) {
    ctx := NewContext()
    ctx.Set("foo", "bar")

    v := ctx.Get("foo")
    if v != "bar" {
        t.Fatalf("expected 'bar', got %v", v)
    }
}

func TestContextGetMissing(t *testing.T) {
    ctx := NewContext()
    v := ctx.Get("missing")
    if v != nil {
        t.Fatalf("expected nil, got %v", v)
    }
}

func TestContextConcurrency(t *testing.T) {
    ctx := NewContext()
    var wg sync.WaitGroup

    for i := 0; i < 100; i++ {
        wg.Add(1)
        go func(i int) {
            defer wg.Done()
            key := fmt.Sprintf("k%d", i)
            ctx.Set(key, i)
            _ = ctx.Get(key)
        }(i)
    }

    wg.Wait()
    // Run with: go test -race
}
