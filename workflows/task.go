package workflows

import (
     "context"
     "errors"
     "log/slog"
)

type Task interface {
    Name() string
    RetryPolicy() RetryPolicy
    Execute(ctx context.Context, wfCtx *Context, logger *slog.Logger) error
}

type TaskFunc struct {
    name string
    pol  RetryPolicy
    fn   func(context.Context, *Context, *slog.Logger) error
}

func NewTask(name string, fn func(context.Context, *Context, *slog.Logger) error, pol RetryPolicy) (*TaskFunc, error) {
    if name == "" {
        name = "UnnamedTask"
    }
    if pol == nil {
        pol = NoRetryPolicy{}
    }
    if fn == nil {
	    return nil, errors.New("Task " + name + " has no execution function")
    }
    return &TaskFunc{name: name, fn: fn, pol: pol}, nil
}

func (t *TaskFunc) Name() string               { return t.name }
func (t *TaskFunc) RetryPolicy() RetryPolicy   { return t.pol }
func (t *TaskFunc) Execute(ctx context.Context, wfCtx *Context, logger *slog.Logger) error {
    return t.fn(ctx, wfCtx, logger)
}
