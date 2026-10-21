package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/sundayprincedev/job-worker-go/internal"
	"github.com/sundayprincedev/job-worker-go/types"
)

// jobRecord is the on-disk shape of a job. Job itself holds atomics and
// derived fields (ID, state), so only the payload is persisted.
type jobRecord struct {
	MerchantAcc string `json:"merchant_acc"`
	UserAcc     string `json:"user_acc"`
	Amount      int64  `json:"amount"`
	CustomerRef string `json:"customer_ref"`
}

// ReadPayloads parses a JSON array of job records and validates each one.
func ReadPayloads(path string) ([]types.PaymentPayload, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read jobs file: %w", err)
	}

	var records []jobRecord
	if err := json.Unmarshal(data, &records); err != nil {
		return nil, fmt.Errorf("parse jobs file %s: %w", path, err)
	}

	payloads := make([]types.PaymentPayload, 0, len(records))
	for i, r := range records {
		if r.MerchantAcc == "" || r.CustomerRef == "" || r.Amount <= 0 {
			return nil, fmt.Errorf("jobs file %s: record %d is invalid (merchant_acc, customer_ref and a positive amount are required)", path, i)
		}
		payloads = append(payloads, types.PaymentPayload{
			MerchantAcc: r.MerchantAcc,
			UserAcc:     r.UserAcc,
			Amount:      r.Amount,
			CustomerRef: r.CustomerRef,
		})
	}
	return payloads, nil
}

// ReadJobs reads the file and registers each payload through the idempotency
// registry. Payloads that map to an already-registered job are skipped, so the
// returned slice never contains the same job twice.
func ReadJobs(path string) ([]*internal.Job, error) {
	payloads, err := ReadPayloads(path)
	if err != nil {
		return nil, err
	}

	jobs := make([]*internal.Job, 0, len(payloads))
	for _, p := range payloads {
		job, loaded := LoadOrStore(p)
		if loaded {
			continue
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}
