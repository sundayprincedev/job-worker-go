package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sundayprincedev/job-worker-go/internal"
	"github.com/sundayprincedev/job-worker-go/types"
)

type Resolver struct {
	StaleAfter time.Duration
	PollEvery  time.Duration

	now func() time.Time
}

func NewResolver() *Resolver {
	return &Resolver{
		StaleAfter: types.UniversalJobTimeout + 50*time.Millisecond,
		PollEvery:  10 * time.Millisecond,
		now:        time.Now,
	}
}

type Resolution struct {
	JobID  string
	From   internal.JobState
	To     internal.JobState
	Reason string
	Err    error
}

func isTerminal(s internal.JobState) bool {
	return s == internal.StateCompleted || s == internal.StateFailed || s == internal.StateCanceled
}

func (r *Resolver) Resolve(ctx context.Context, job *internal.Job) (res Resolution) {
	res = Resolution{JobID: job.ID, From: internal.JobState(job.State.Load())}
	waited := false
	defer func() { ejectIfTerminal(job) }()

	for {
		state := internal.JobState(job.State.Load())

		switch {
		case isTerminal(state):
			res.To = state
			if waited {
				res.Reason = "owning worker finished it"
			} else {
				res.Reason = "already terminal"
			}
			return res

		case state == internal.StateCommitted:
			// Ledger write is confirmed; only the close-out is missing.
			if job.Transition(internal.StateCompleted) {
				res.To = internal.StateCompleted
				res.Reason = "ledger commit confirmed but job was never closed; completed without re-running"
				return res
			}

		case state == internal.StatePending || state == internal.StateRunning:
			since := job.CreatedAt
			if state == internal.StateRunning {
				since = time.Unix(0, job.UpdatedAt.Load())
			}
			idle := r.now().Sub(since)
			if idle >= r.StaleAfter {
				// Neither state reached the ledger commit, so failing is safe: no money moved.
				if job.Transition(internal.StateFailed) {
					res.To = internal.StateFailed
					res.Reason = fmt.Sprintf("%s with no progress for %s; owner presumed lost before commit", stateName(state), idle.Round(time.Millisecond))
					res.Err = internal.ErrStaleJob
					return res
				}
				continue
			}
			waited = true
			select {
			case <-ctx.Done():
				res.To = state
				res.Reason = "gave up waiting for the owning worker"
				res.Err = context.Cause(ctx)
				return res
			case <-time.After(r.PollEvery):
			}

		default:
			res.To = state
			res.Reason = "unknown state"
			res.Err = fmt.Errorf("%w: state %d", internal.ErrInvalidTransition, state)
			return res
		}
	}
}

func (r *Resolver) ResolveAll(ctx context.Context, jobs []*internal.Job, onDone func(Resolution)) []Resolution {
	out := make([]Resolution, len(jobs))
	var (
		wg sync.WaitGroup
		mu sync.Mutex
	)
	for i, job := range jobs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res := r.Resolve(ctx, job)
			out[i] = res
			if onDone != nil {
				mu.Lock()
				onDone(res)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return out
}
