# Job Worker Go

Concurrency skeleton for double-entry ledger systems.

## Current State (Oct 21, 2026)

✅ Atomic job state machine with CAS transitions  
✅ Deterministic payment payload (MerchantID, Amount, CustomerRef)  
✅ sync.Map idempotency registry with LoadOrStore routing  
✅ Two-path architecture:  
   - Fresh jobs → main processor fan-out to worker pool  
   - Existing jobs → resolver drives to terminal state safely  
✅ Triple-entry ledger writes enforced atomically:  
   1. Debit suspense account (amount + commission)  
   2. Credit merchant account  
   3. Credit house commission account (20% fixed rate)  
✅ Jobs loaded from a JSON file (`jobs.json`, 10 sample payments) via `reader.go`  
✅ `resolver.go`: non-pending jobs are driven to a terminal state (stale Running → Failed, Committed → Completed without re-running, live workers waited on)  
✅ Simulated processor failures (fraud flagged, insufficient funds) returned as typed errors  
✅ CLI demo with live log streaming and SIGINT/SIGTERM handling  
✅ Tests for the reader, state transitions, cancellation and failure paths (run with `-race`)  

## Running It

```bash
go run . -n 5                      # run the first 5 jobs
go run . -n all                    # run everything in jobs.json
go run . -fail-rate 0.5            # processor declines half the jobs
go run . -inject-signal 60ms       # send SIGINT mid-run to watch the shutdown
go run . -n 7 -inject-leftovers    # simulate crashed-run leftovers so the resolver has work
go test -race ./...
```

Flags: `-file` (jobs file), `-n` (count or `all`), `-fail-rate` (0 to 1, default 0.25), `-inject-signal` (delay, off by default), `-inject-leftovers` (demo the resolver path).

Heads up: the pool has a 100ms timeout and some simulated work takes longer, so a few jobs will time out even with `-fail-rate 0`. That's expected to see context deadline hits as it should

## Architecture Decisions

- **No UUIDs**: Job IDs derived deterministically from payload
- **System constants**: House account ID and commission rate live outside payload—no caller manipulation possible  
- **Terminal state ejection**: Jobs removed from registry once terminal. The `LoadOrStore` loaded flag routes a job: created by this call → worker pool, already registered → resolver. (Ledger-confirmed ejection comes with the ledger layer.)  
- **Panic isolation**: Per-goroutine recovery with stack trace capture before memory reuse  

## What's Next

- [ ] Worker pool lifecycle with bounded channels and a proper SIGTERM drain test 
- [ ] Crash window handling between ledger commit and ACK send
- [ ] Retry boundary test proving no double-posting after process restart  
- [ ] Append-only journal integration for durable state recovery  
- [ ] Race detector validation under concurrent retries + injected panics  

## Status

🚧 Active development — updates committed daily with tests  
🔍 Peer review welcome — especially on crash window handling and worker drain semantics  
📜 License: MIT (when stable)

---

*“Silence looks too much like fraud to ignore.”*
