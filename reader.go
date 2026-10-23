package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/sundayprincedev/job-worker-go/internal"
	"github.com/sundayprincedev/job-worker-go/types"
)

type jobRecord struct {
	MerchantAcc string `json:"merchant_acc"`
	UserAcc     string `json:"user_acc"`
	Amount      int64  `json:"amount"`
	CustomerRef string `json:"customer_ref"`
}

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

type Intake struct {
	Fresh    []*internal.Job
	Existing []*internal.Job
}

func ReadJobs(path string, limit int) (Intake, error) {
	payloads, err := ReadPayloads(path)
	if err != nil {
		return Intake{}, err
	}
	if limit > 0 && limit < len(payloads) {
		payloads = payloads[:limit]
	}

	var in Intake
	seen := make(map[*internal.Job]struct{}, len(payloads))
	for _, p := range payloads {
		job, loaded := LoadOrStore(p)
		if _, dup := seen[job]; dup {
			continue
		}
		seen[job] = struct{}{}
		if loaded {
			in.Existing = append(in.Existing, job)
		} else {
			in.Fresh = append(in.Fresh, job)
		}
	}
	return in, nil
}
