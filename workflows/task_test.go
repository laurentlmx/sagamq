package workflows

import (
    "context"
    "log/slog"
    "os"
    "testing"
)

var taskLogger = slog.New(slog.NewTextHandler(os.Stdout, nil))

func TestTaskFuncExecutes(t *testing.T) {
    called := false

    task, err := NewTask("TestTask", func(ctx context.Context, c *Context, logger  *slog.Logger) error {
        called = true
        return nil
    }, nil)
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }

    if task.Name() != "TestTask" {
        t.Fatalf("expected name 'TestTask', got %q", task.Name())
    }

    if _, ok := task.RetryPolicy().(NoRetryPolicy); !ok {
        t.Fatalf("expected default NoRetryPolicy")
    }

    errExec := task.Execute(context.Background(), NewContext(), taskLogger)
    if errExec != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    if !called {
        t.Fatalf("expected task to be called")
    }
}

func TestTaskNilFunc(t *testing.T) {
    _, err := NewTask("TestTask", nil, nil)
    if err == nil {
            t.Fatalf("expected error due to 'nil' execution function")
    }
}
