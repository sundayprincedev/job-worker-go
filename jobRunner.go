package main

import (
	"context"
	"sync"

	"github.com/sundayprincedev/job-worker-go/internal"
	"github.com/sundayprincedev/job-worker-go/types"
)

type Observer struct {
	OnResult  func(internal.JobResult)
	OnError   func(internal.JobError)
	OnResolve func(Resolution)
}

func RunJobs(parent context.Context, jobs []*internal.Job) error {
	return RunJobsObserved(parent, jobs, Observer{})
}

func RunJobsObserved(parent context.Context, jobs []*internal.Job, obs Observer) error {
	wp := NewWorkerPool(parent, len(jobs), types.WorkerCount*2, len(jobs))
	wp.Start()

	go func() {
		defer close(wp.JobChan)
		for _, job := range jobs {
			select {
			case <-wp.Ctx.Done():
				return
			case wp.JobChan <- job:
			}
		}
	}()

	var (
		results []internal.JobResult
		errs    []error
	)

	resultsOpen, errsOpen := true, true
	parentDone := parent.Done()
	for resultsOpen || errsOpen {
		select {
		case r, ok := <-wp.ResultsChan:
			if !ok {
				resultsOpen = false
				continue
			}
			results = append(results, r)
			ejectByID(r.JobID) // terminal: the job leaves the registry
			if obs.OnResult != nil {
				obs.OnResult(r)
			}
			// Ledger / bookkeeping layer per finished job
			// reconcileByJobID(r.JobID, r.Success, r.Err)

		case e, ok := <-wp.ErrChan:
			if !ok {
				errsOpen = false
				continue
			}
			errs = append(errs, &e)
			if obs.OnError != nil {
				obs.OnError(e)
			}
			// Error book layer
			// recordError(e)

		case <-parentDone:
			wp.Cancel()
			parentDone = nil // fire once; a closed channel would otherwise spin this loop
		}
	}

	wp.Wg.Wait()

	if len(errs) > 0 {
		return errs[0]
	}
	_ = results
	// Jobs dropped because the parent was canceled produce no errors of their own.
	return parent.Err()
}

type DispatchResult struct {
	Fresh       int
	Swept       int
	RunErr      error
	Resolutions []Resolution
}

func sweepRegistry(skip map[*internal.Job]struct{}) []*internal.Job {
	var out []*internal.Job
	JobRegistry.Range(func(_, v any) bool {
		if j := v.(*internal.Job); j != nil {
			if _, skipped := skip[j]; !skipped {
				out = append(out, j)
			}
		}
		return true
	})
	return out
}

func Dispatch(ctx context.Context, in Intake, rv *Resolver, obs Observer) DispatchResult {
	known := make(map[*internal.Job]struct{}, len(in.Fresh)+len(in.Existing))
	for _, j := range in.Fresh {
		known[j] = struct{}{}
	}
	for _, j := range in.Existing {
		known[j] = struct{}{}
	}
	swept := sweepRegistry(known)
	toResolve := append(append([]*internal.Job{}, in.Existing...), swept...)

	res := DispatchResult{Fresh: len(in.Fresh), Swept: len(swept)}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if len(in.Fresh) > 0 {
			res.RunErr = RunJobsObserved(ctx, in.Fresh, obs)
		}
	}()
	go func() {
		defer wg.Done()
		res.Resolutions = rv.ResolveAll(ctx, toResolve, obs.OnResolve)
	}()
	wg.Wait()
	return res
}
