package main

import (
	"context"

	"github.com/sundayprincedev/job-worker-go/internal"
	"github.com/sundayprincedev/job-worker-go/types"
)

type Observer struct {
	OnResult func(internal.JobResult)
	OnError  func(internal.JobError)
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
