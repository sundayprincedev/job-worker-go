package main

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sundayprincedev/job-worker-go/internal"
	"github.com/sundayprincedev/job-worker-go/types"
)

var JobRegistry sync.Map

func generateJobId(payload types.PaymentPayload) string {
	var b strings.Builder
	b.WriteString(payload.MerchantID)
	b.WriteString("-")
	b.WriteString(strconv.FormatInt(payload.Amount, 10))
	b.WriteString("-")
	b.WriteString(payload.CustomerRef)

	return b.String()
}

func LoadOrStore(payload types.PaymentPayload) (*internal.Job, bool) {
	jobID := generateJobId(payload)

	newJob := &internal.Job{
		ID:        jobID,
		Payload:   payload,
		Timeout:   types.UniversalJobTimeout,
		CreatedAt: time.Now().UTC(),
	}

	newJob.State.Store(int32(internal.StatePending))

	actual, loaded := JobRegistry.LoadOrStore(jobID, newJob)

	return actual.(*internal.Job), loaded
}

type WorkerPool struct {
	JobChan     chan *internal.Job
	ErrChan     chan internal.JobError
	ResultsChan chan internal.JobResult
	Wg          sync.WaitGroup
	Ctx         context.Context
	Cancel      context.CancelFunc
}

func NewWorkerPool(parent context.Context, buffer, errBuf, resultBuf int) *WorkerPool {
	ctx, cancel := context.WithTimeoutCause(parent, types.UniversalJobTimeout, internal.ErrAllFailed)
	return &WorkerPool{
		JobChan:     make(chan *internal.Job, buffer),
		ErrChan:     make(chan internal.JobError, errBuf),
		ResultsChan: make(chan internal.JobResult, resultBuf),
		Ctx:         ctx,
		Cancel:      cancel,
	}
}

func (wp *WorkerPool) Start() {
	workers := types.WorkerCount
	wp.Wg.Add(workers)

	for range workers {
		go func() {
			defer wp.Wg.Done()
			for job := range wp.JobChan {
				if err := executeJob(wp.Ctx, job); err != nil {
					from := internal.JobState(job.State.Load())
					if ok := job.Transition(internal.StateFailed); !ok {
						err := internal.NewTransitionError(job.ID, from, internal.StateFailed, "unexpected state", nil)
						wp.ErrChan <- *internal.NewJobError(job.ID, "unexpected transition state", err)
					}

					wp.ErrChan <- *internal.NewJobError(job.ID, "job failed", err)
				}
			}
		}()
	}

	go func() {
		wp.Wg.Wait()
		wp.Cancel()
		close(wp.JobChan)
		close(wp.ErrChan)
	}()
}

func executeJob(ctx context.Context, job *internal.Job) error {
	//change job state to running before any execution
	return nil
}
