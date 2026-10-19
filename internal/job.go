package internal

import (
	"sync/atomic"
	"time"

	"github.com/sundayprincedev/job-worker-go/types"
)

type JobState int32

const (
	StatePending JobState = iota
	StateRunning
	StateCommitted
	StateCompleted
	StateFailed
	StateCanceled
)

type Job struct {
	ID        string
	Payload   types.PaymentPayload
	Timeout   time.Duration
	State     atomic.Int32
	Result    []byte
	Err       error
	CreatedAt time.Time
	UpdatedAt atomic.Int64
}

func (j *Job) Transition(to JobState) bool {
	for {
		current := JobState(j.State.Load())
		if current >= to {
			return false
		}
		if j.State.CompareAndSwap(int32(current), int32(to)) {
			j.UpdatedAt.Store(time.Now().UnixNano())
			return true
		}
	}
}
