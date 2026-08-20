package workflows

import (
    "context"
    "errors"
    "log/slog"
    "os"
    "reflect"
    "testing"
    "time"
)

var testLogger = slog.New(slog.NewTextHandler(os.Stdout, nil))

func TestUnnamedWorkflow(t *testing.T) {
    engine, _ := NewWorkflowEngine("", []Task{}, nil, testLogger)

    if engine.Name() != "UnnamedWorkflow" {
        t.Fatalf("expected UnnamedWorkflow, got %v", engine.Name())
    }
}

func TestNoTasks(t *testing.T) {
    _, err := NewWorkflowEngine("NoTasks", nil, nil, testLogger)
    if err == nil {
        t.Fatalf("expected error, got nil")
    }
}

func TestTaskNotFound(t *testing.T) {
    engine, _ := NewWorkflowEngine("TaskNotFound", []Task{}, nil, testLogger)

    err := engine.Run(context.Background(), "Unknown", NewContext())
    if err == nil {
        t.Fatalf("expected error, got nil")
    }
}

func TestSimpleWorkflow(t *testing.T) {
    wfCtx := NewContext()
    wfCtx.Set("count", 0)

    inc, _ := NewTask("inc", func(ctx context.Context, c *Context, logger  *slog.Logger) error {
        v := c.Get("count").(int)
        c.Set("count", v+1)
        return nil
    }, NoRetryPolicy{})

    engine, _ := NewWorkflowEngine(
	"SimpleWorkflow",
	[]Task{inc},
        map[string]TransitionFunc{
            "inc": func(c *Context, err error, logger  *slog.Logger) (string, error) {
                if c.Get("count").(int) >= 3 {
                    return "", nil
                }
                return "inc", nil
            },
        },
	testLogger,
    )

    err := engine.Run(context.Background(), "inc", wfCtx)
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }

    if wfCtx.Get("count").(int) != 3 {
        t.Fatalf("expected count=3, got %v", wfCtx.Get("count"))
    }
}

func TestTransitionsOrder(t *testing.T) {
    order := []string{}

    t1, _ := NewTask("A", func(ctx context.Context, c *Context, logger  *slog.Logger) error {
        order = append(order, "A")
        return nil
    }, nil)

    t2, _ := NewTask("B", func(ctx context.Context, c *Context, logger  *slog.Logger) error {
        order = append(order, "B")
        return nil
    }, nil)

    engine, _ := NewWorkflowEngine(
	"TransitionsOrder",
        []Task{t1, t2},
        map[string]TransitionFunc{
            "A": func(c *Context, err error, logger  *slog.Logger) (string, error) { return "B", nil },
            "B": func(c *Context, err error, logger  *slog.Logger) (string, error) { return "", nil },
        },
	testLogger,
    )

    err := engine.Run(context.Background(), "A", NewContext())
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }

    expected := []string{"A", "B"}
    if !reflect.DeepEqual(order, expected) {
        t.Fatalf("expected order %v, got %v", expected, order)
    }
}

func TestTransitionReturnErr(t *testing.T) {
    task, _ := NewTask("ErrTask", func(ctx context.Context, c *Context, logger  *slog.Logger) error {
        return errors.New("Some error")
    }, nil)

    engine, _ := NewWorkflowEngine(
        "TransitionReturnErr",
        []Task{task},
        map[string]TransitionFunc{
            "ErrTask": func(c *Context, err error, logger  *slog.Logger) (string, error) {
		logger.Warn("Propagating received error", "error", err)
		return "", err
	    },
        },
        testLogger,
    )

    err := engine.Run(context.Background(), "ErrTask", NewContext())
    if err == nil {
        t.Fatalf("expected error %v, got nil", err)
    }
}

func TestCancellation(t *testing.T) {
    ctx, cancel := context.WithCancel(context.Background())
    cancel()

    task, _ := NewTask("X", func(ctx context.Context, c *Context, logger  *slog.Logger) error {
        return nil
    }, nil)

    engine, _ := NewWorkflowEngine("Cancellation", []Task{task}, nil, testLogger)

    err := engine.Run(ctx, "X", NewContext())
    if err == nil {
        t.Fatalf("expected cancellation error, got nil")
    }
}

func TestEngineTaskFailureStopsWorkflow(t *testing.T) {
    failTask, _ := NewTask("Fail", func(ctx context.Context, c *Context, logger  *slog.Logger) error {
        return errors.New("boom")
    }, NoRetryPolicy{})

    engine, _ := NewWorkflowEngine("EngineTaskFailureStopsWorkflow", []Task{failTask}, nil, testLogger)

    err := engine.Run(context.Background(), "Fail", NewContext())
    if err == nil {
        t.Fatalf("expected error, got nil")
    }
}

func TestEngineRetryWithRealPolicy(t *testing.T) {
    attempts := 0

    task, _ := NewTask("RetryTask", func(ctx context.Context, c *Context, logger  *slog.Logger) error {
        attempts++
        if attempts < 3 {
            return errors.New("temporary")
        }
        return nil
    }, NewExponentialBackoffPolicy(1*time.Millisecond, 10*time.Millisecond))

    engine, _ := NewWorkflowEngine("EngineRetryWithRealPolicy", []Task{task}, nil, testLogger)

    err := engine.Run(context.Background(), "RetryTask", NewContext())
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }

    if attempts != 3 {
        t.Fatalf("expected 3 attempts, got %d", attempts)
    }
}

func TestEngineTaskRepetedFailuresStopsWorkflow(t *testing.T) {
    attempts := 0

    task, _ := NewTask("KeepFailingTask", func(ctx context.Context, c *Context, logger  *slog.Logger) error {
        attempts++
        return errors.New("keep failing...")
    }, NewJitterRetryPolicy(2, 1*time.Millisecond, 0))

    engine, _ := NewWorkflowEngine("EngineTaskRepetedFailuresStopsWorkflow", []Task{task}, nil, testLogger)

    err := engine.Run(context.Background(), "KeepFailingTask", NewContext())
    if err == nil {
        t.Fatalf("expected error, got nil")
    }

    if attempts != 3 {
        t.Fatalf("expected 3 attempts, got %d", attempts)
    }
}
