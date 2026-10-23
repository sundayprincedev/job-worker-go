package internal

import (
	"errors"
	"fmt"
	"time"
)

var (
	ErrFraudDetected     = errors.New("processor flagged transaction as fraud")
	ErrInsufficientFunds = errors.New("processor declined: insufficient funds")
	ErrAllFailed         = errors.New("all processors failed or timed out")
	ErrInvalidTransition = errors.New("invalid job state transition")
	ErrStaleJob          = errors.New("job made no progress; owning worker presumed lost")
)

type JobError struct {
	JobID     string
	Reason    string
	Err       error
	Stack     string
	Timestamp time.Time
}

func (e *JobError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("job %s: %s: %v", e.JobID, e.Reason, e.Err)
	}
	return fmt.Sprintf("job %s: %s", e.JobID, e.Reason)
}

func (e *JobError) Unwrap() error {
	return e.Err
}

func NewJobError(jobID, reason string, err error) *JobError {
	return &JobError{
		JobID:     jobID,
		Reason:    reason,
		Err:       err,
		Timestamp: time.Now(),
	}
}

func NewJobErrorWithStack(jobID, reason string, err error, stack string) *JobError {
	return &JobError{
		JobID:     jobID,
		Reason:    reason,
		Err:       err,
		Stack:     stack,
		Timestamp: time.Now(),
	}
}

type ProcessorError struct {
	Processor string
	Reason    string
	Err       error
	Stack     string
	Timestamp time.Time
	JobID     string
}

func (e *ProcessorError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %s: %v", e.Processor, e.Reason, e.Err)
	}
	return fmt.Sprintf("%s: %s", e.Processor, e.Reason)
}

func (e *ProcessorError) Unwrap() error { return e.Err }

type TransitionError struct {
	JobID     string
	From      JobState
	To        JobState
	Reason    string
	Err       error
	Timestamp time.Time
}

func (e *TransitionError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("job %s: transition %d -> %d: %s: %v", e.JobID, e.From, e.To, e.Reason, e.Err)
	}
	return fmt.Sprintf("job %s: transition %d -> %d: %s", e.JobID, e.From, e.To, e.Reason)
}

func (e *TransitionError) Unwrap() error {
	if e.Err != nil {
		return e.Err
	}
	return ErrInvalidTransition
}

func NewTransitionError(jobID string, from, to JobState, reason string, err error) *TransitionError {
	return &TransitionError{
		JobID:     jobID,
		From:      from,
		To:        to,
		Reason:    reason,
		Err:       err,
		Timestamp: time.Now(),
	}
}
