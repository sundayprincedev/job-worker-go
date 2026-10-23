package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"runtime"
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
	b.WriteString(payload.MerchantAcc)
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

func ejectIfTerminal(job *internal.Job) bool {
	if job == nil || !isTerminal(internal.JobState(job.State.Load())) {
		return false
	}
	return JobRegistry.CompareAndDelete(job.ID, job)
}

func ejectByID(id string) {
	if v, ok := JobRegistry.Load(id); ok {
		ejectIfTerminal(v.(*internal.Job))
	}
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
				// If work finished but ledger write hadn't run yet, and the worker panic,
				// recover here, push a rich error (stage + stack), and let the
				// coordinator cross-check ResultsChan / job state for this JobID
				// so bookkeeping can reconcile "done but not written"...
				func(job *internal.Job) {
					defer func() {
						if r := recover(); r != nil {
							buf := make([]byte, 4096)
							n := runtime.Stack(buf, false)
							stackTrace := string(buf[:n])
							wp.ErrChan <- *internal.NewJobErrorWithStack(
								job.ID, "panic", fmt.Errorf("%v", r), stackTrace,
							)
							//Ledger side here, later.
							// Coordinator should also inspect ResultsChan + job.State
							// for this JobID to see whether transfer already happened before anything else...
						}
					}()

					err := executeJob(wp.Ctx, job)
					if err != nil {
						from := internal.JobState(job.State.Load())
						if ok := job.Transition(internal.StateFailed); !ok {
							terr := internal.NewTransitionError(job.ID, from, internal.StateFailed, "unexpected state", nil)
							wp.ErrChan <- *internal.NewJobError(job.ID, "unexpected transition state", terr)
						}
						wp.ErrChan <- *internal.NewJobError(job.ID, "job failed", err)
						wp.ResultsChan <- internal.JobResult{JobID: job.ID, Success: false, Err: err}
						return
					}

					wp.ResultsChan <- internal.JobResult{JobID: job.ID, Success: true, Err: nil}
				}(job)
			}
		}()
	}

	go func() {
		wp.Wg.Wait()
		wp.Cancel()
		close(wp.ErrChan)
		close(wp.ResultsChan)
	}()
}

func executeJob(ctx context.Context, job *internal.Job) error {
	from := internal.JobState(job.State.Load())

	if err := ctx.Err(); err != nil {
		return err
	}

	if ok := job.Transition(internal.StateRunning); !ok {
		err := internal.NewTransitionError(job.ID, from, internal.StateRunning, "unexpected state", nil)
		return err
	}

	jobCtx, cancel := context.WithTimeout(ctx, types.UniversalJobTimeout)
	defer cancel()

	if err := jobCtx.Err(); err != nil {
		return err
	}

	if err := doWork(jobCtx, job); err != nil {
		return err
	}

	if err := jobCtx.Err(); err != nil {
		// Ledger write: work finished but context died before write
		return err
	}

	// Ledger layer: write success (Result, CustomerRef, accounts, amount)

	if ok := job.Transition(internal.StateCompleted); !ok {
		err := internal.NewTransitionError(job.ID, from, internal.StateCompleted, "unexpected state", nil)
		return err
	}

	return nil
}

var FailureRate = 0.25

var processorFailures = []struct {
	reason string
	err    error
}{
	{"fraud check", internal.ErrFraudDetected},
	{"balance check", internal.ErrInsufficientFunds},
}

func simulateFailure(job *internal.Job) error {
	if rand.Float64() >= FailureRate {
		return nil
	}
	f := processorFailures[rand.IntN(len(processorFailures))]
	return &internal.ProcessorError{
		Processor: "sim-processor",
		Reason:    f.reason,
		Err:       f.err,
		JobID:     job.ID,
		Timestamp: time.Now(),
	}
}

func doWork(ctx context.Context, job *internal.Job) error {
	if perr := simulateFailure(job); perr != nil {
		select {
		case <-time.After(20 * time.Millisecond):
			job.Err = perr
			return perr
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	//This is a simulation of actual work with delays..
	delays := []time.Duration{
		20 * time.Millisecond,
		50 * time.Millisecond,
		80 * time.Millisecond,
		100 * time.Millisecond,
		101 * time.Millisecond,
		150 * time.Millisecond,
	}
	delay := delays[time.Now().UnixNano()%int64(len(delays))]

	select {
	case <-time.After(delay):
		job.Result = []byte("transfer_ok")
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
