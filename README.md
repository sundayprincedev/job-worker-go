# Job Worker Go

Concurrency skeleton for double-entry ledger systems. Built to survive Black Friday volume, grid failures, and regulatory audits.

## Current State (Oct 19, 2026)

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

## Architecture Decisions

- **No UUIDs**: Job IDs derived deterministically from payload
- **System constants**: House account ID and commission rate live outside payload—no caller manipulation possible  
- **Terminal state ejection**: Jobs removed from registry only after ledger write confirmed (success or failure journal)  
- **Panic isolation**: Per-goroutine recovery with stack trace capture before memory reuse  

## What's Next

- [ ] Worker pool lifecycle with bounded channels and SIGTERM drain tests 
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
