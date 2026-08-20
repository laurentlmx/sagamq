package workflows

import (
    "context"
    "errors"
    "log/slog"
    "os"
    "time"
)

type TransitionFunc func(*Context, error, *slog.Logger) (string, error)

type WorkflowEngine struct {
    name        string
    tasks       map[string]Task
    transitions map[string]TransitionFunc
    logger      *slog.Logger
}

func (e *WorkflowEngine) Name() string { return e.name }

func NewWorkflowEngine(name string, tasks []Task, transitions map[string]TransitionFunc, logger *slog.Logger) (*WorkflowEngine, error) {
    if name == "" {
        name = "UnnamedWorkflow"
    }

    if tasks == nil {
	return nil, errors.New("Workflow " + name + " has no tasks")
    }

    taskMap := make(map[string]Task, len(tasks))
    for _, t := range tasks {
        taskMap[t.Name()] = t
    }

    if logger == nil {
	logger = slog.New(slog.NewTextHandler(os.Stdout, nil))
    }

    return &WorkflowEngine{name: name, tasks: taskMap, transitions: transitions, logger: logger}, nil
}

func (e *WorkflowEngine) Run(ctx context.Context, start string, wfCtx *Context) error {
    if wfCtx == nil {
        wfCtx = NewContext()
    }

    current := start
    workflowStart := time.Now()
    e.logger.Info("Starting workflow", "name", e.name, "start", workflowStart)

    for current != "" {
        select {
        case <-ctx.Done():
            return ctx.Err()
        default:
        }

        task, ok := e.tasks[current]
        if !ok {
            return errors.New("task not found: " + current)
        }

	taskStart := time.Now()
        e.logger.Info("Starting task", "name", task.Name(), "start", taskStart)
        policy := task.RetryPolicy()
        retryStart := time.Now()
        attempts := 0
	var err error

        for {
            err = task.Execute(ctx, wfCtx, e.logger)
            if err == nil {
		taskEnd := time.Now()
        	e.logger.Info("Task completed", "name", task.Name(), "end", taskEnd)
                break
            }

            attempts++
            elapsed := time.Since(retryStart)

            if !policy.ShouldRetry(attempts, elapsed) {
		e.logger.Warn("task failed", "task", task.Name(), "error", err, "retry", "no")
		break
            }

            delay := policy.Delay(attempts)
            e.logger.Warn("Task failed", "task", task.Name(), "error", err, "retry_in", delay)

            timer := time.NewTimer(delay)
            select {
            case <-ctx.Done():
                timer.Stop()
                return ctx.Err()
            case <-timer.C:
            }
        }

	next := e.transitions[current]
	if next == nil {
            if err == nil {
               current = ""
	    } else {
	       return err
            }
        } else {
            current, err = next(wfCtx, err, e.logger)
	    if err != nil {
		return err
	    }
        }
    }

    workflowEnd := time.Now()
    e.logger.Info("Workflow completed", "name", e.name, "end", workflowEnd)
    return nil
}
