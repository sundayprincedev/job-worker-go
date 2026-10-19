package main

import (
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
