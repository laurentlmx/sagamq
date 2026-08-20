package workflows

import "sync"

type Context struct {
    mu   sync.RWMutex
    data map[string]any
}

func NewContext() *Context {
    return &Context{
        data: make(map[string]any),
    }
}

func (c *Context) Set(key string, value any) {
    c.mu.Lock()
    defer c.mu.Unlock()
    c.data[key] = value
}

func (c *Context) Get(key string) any {
    c.mu.RLock()
    defer c.mu.RUnlock()
    return c.data[key]
}
