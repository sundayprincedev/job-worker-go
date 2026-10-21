package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sundayprincedev/job-worker-go/internal"
	"github.com/sundayprincedev/job-worker-go/types"
)

var uniq atomic.Int64

func unique(ref string) string { return ref + "-" + strconv.FormatInt(uniq.Add(1), 10) }

func payload(ref string, amount int64) types.PaymentPayload {
	return types.PaymentPayload{MerchantAcc: "M_" + ref, UserAcc: "U", Amount: amount, CustomerRef: ref}
}

func newJob(ref string) *internal.Job {
	p := payload(unique(ref), 100)
	return &internal.Job{ID: generateJobId(p), Payload: p, Timeout: types.UniversalJobTimeout}
}

func writeFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "jobs.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestGenerateJobIDDeterministic(t *testing.T) {
	p := payload("X", 5)
	if generateJobId(p) != generateJobId(p) {
		t.Fatal("same payload must yield the same ID")
	}
	if generateJobId(p) == generateJobId(payload("Y", 5)) {
		t.Fatal("different payloads must yield different IDs")
	}
}

func TestLoadOrStoreIdempotent(t *testing.T) {
	p := payload(unique("idem"), 42)
	first, loaded := LoadOrStore(p)
	if loaded {
		t.Fatal("first call must create a new job")
	}
	second, loaded := LoadOrStore(p)
	if !loaded || first != second {
		t.Fatal("second call must return the existing job")
	}
	if got := internal.JobState(first.State.Load()); got != internal.StatePending {
		t.Fatalf("new job state = %d, want pending", got)
	}
}

func TestReadJobsShippedFile(t *testing.T) {
	payloads, err := ReadPayloads("jobs.json")
	if err != nil {
		t.Fatal(err)
	}
	if len(payloads) != 10 {
		t.Fatalf("jobs.json has %d jobs, want 10", len(payloads))
	}
}

func TestReadPayloadsErrors(t *testing.T) {
	cases := map[string]string{
		"bad json":       `{not json`,
		"missing amount": `[{"merchant_acc":"M","customer_ref":"R"}]`,
		"missing ref":    `[{"merchant_acc":"M","amount":1}]`,
	}
	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := ReadPayloads(writeFile(t, content)); err == nil {
				t.Fatal("expected error")
			}
		})
	}
	if _, err := ReadPayloads(filepath.Join(t.TempDir(), "missing.json")); err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestReadJobsSkipsDuplicates(t *testing.T) {
	ref := unique("dup")
	path := writeFile(t, `[
		{"merchant_acc":"M","amount":1,"customer_ref":"`+ref+`"},
		{"merchant_acc":"M","amount":1,"customer_ref":"`+ref+`"}
	]`)
	jobs, err := ReadJobs(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 {
		t.Fatalf("got %d jobs, want 1 after dedup", len(jobs))
	}
}

func TestSelectJobs(t *testing.T) {
	jobs := []*internal.Job{newJob("a"), newJob("b"), newJob("c")}
	for _, tc := range []struct {
		n    string
		want int
		err  bool
	}{
		{"all", 3, false}, {"2", 2, false}, {"99", 3, false},
		{"0", 0, true}, {"-1", 0, true}, {"abc", 0, true},
	} {
		got, err := selectJobs(jobs, tc.n)
		if (err != nil) != tc.err || len(got) != tc.want {
			t.Errorf("selectJobs(%q) = %d, err=%v; want %d, err=%v", tc.n, len(got), err, tc.want, tc.err)
		}
	}
}

func TestJobTransitionOnlyForward(t *testing.T) {
	j := newJob("t")
	if !j.Transition(internal.StateRunning) {
		t.Fatal("pending -> running must succeed")
	}
	if j.Transition(internal.StateRunning) || j.Transition(internal.StatePending) {
		t.Fatal("repeat or backward transition must fail")
	}
}

func TestExecuteJobCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	j := newJob("canceled")
	if err := executeJob(ctx, j); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if got := internal.JobState(j.State.Load()); got != internal.StatePending {
		t.Fatalf("state = %d, want pending (never started)", got)
	}
}

func TestExecuteJobRejectsReplay(t *testing.T) {
	j := newJob("replay")
	j.State.Store(int32(internal.StateCompleted))
	err := executeJob(context.Background(), j)
	if !errors.Is(err, internal.ErrInvalidTransition) {
		t.Fatalf("err = %v, want ErrInvalidTransition", err)
	}
}

// doWork picks a random delay (some exceed the job timeout), so a run may
// succeed or time out. Both are valid; what must hold is that every job
// reaches a terminal state, results line up, and nothing hangs.
func TestRunJobsReachesTerminalStates(t *testing.T) {
	jobs := make([]*internal.Job, 8)
	for i := range jobs {
		jobs[i] = newJob("run-" + string(rune('a'+i)))
	}

	var results, errs int
	done := make(chan error, 1)
	go func() {
		done <- RunJobsObserved(context.Background(), jobs, Observer{
			OnResult: func(internal.JobResult) { results++ },
			OnError:  func(internal.JobError) { errs++ },
		})
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("RunJobs did not return (deadlock?)")
	}

	if results > len(jobs) {
		t.Fatalf("got %d results for %d jobs", results, len(jobs))
	}
	for _, j := range jobs {
		s := internal.JobState(j.State.Load())
		if s == internal.StateRunning {
			t.Errorf("job %s stuck in running", j.ID)
		}
	}
}

func TestRunJobsPreCanceledParent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	jobs := []*internal.Job{newJob("pc-1"), newJob("pc-2"), newJob("pc-3")}

	done := make(chan error, 1)
	go func() { done <- RunJobs(ctx, jobs) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected an error when the parent context is already canceled")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RunJobs did not return after cancellation")
	}
	for _, j := range jobs {
		if s := internal.JobState(j.State.Load()); s == internal.StateCompleted {
			t.Errorf("job %s completed despite canceled context", j.ID)
		}
	}
}

func TestRunJobsEmpty(t *testing.T) {
	if err := RunJobs(context.Background(), nil); err != nil {
		t.Fatalf("empty run returned %v", err)
	}
}

func setFailureRate(t *testing.T, r float64) {
	t.Helper()
	old := FailureRate
	FailureRate = r
	t.Cleanup(func() { FailureRate = old })
}

func TestSimulatedFailureAlways(t *testing.T) {
	setFailureRate(t, 1)
	j := newJob(unique("fail"))
	err := executeJob(context.Background(), j)
	if err == nil {
		t.Fatal("expected a simulated failure")
	}
	var perr *internal.ProcessorError
	if !errors.As(err, &perr) || perr.JobID != j.ID {
		t.Fatalf("err = %v, want *ProcessorError for job %s", err, j.ID)
	}
	if !errors.Is(err, internal.ErrFraudDetected) && !errors.Is(err, internal.ErrInsufficientFunds) {
		t.Fatalf("err = %v, want fraud or insufficient funds", err)
	}
	if j.Err == nil {
		t.Fatal("job.Err should record the failure")
	}
}

func TestSimulatedFailureNever(t *testing.T) {
	setFailureRate(t, 0)
	for range 50 {
		if err := simulateFailure(newJob("x")); err != nil {
			t.Fatalf("unexpected failure at rate 0: %v", err)
		}
	}
}

func TestRunJobsReportsSimulatedFailures(t *testing.T) {
	setFailureRate(t, 1)
	jobs := []*internal.Job{newJob(unique("sf")), newJob(unique("sf")), newJob(unique("sf"))}

	var failed int
	err := RunJobsObserved(context.Background(), jobs, Observer{
		OnResult: func(r internal.JobResult) {
			if !r.Success && (errors.Is(r.Err, internal.ErrFraudDetected) || errors.Is(r.Err, internal.ErrInsufficientFunds)) {
				failed++
			}
		},
	})
	if err == nil || failed != len(jobs) {
		t.Fatalf("err=%v failed=%d, want error and %d processor failures", err, failed, len(jobs))
	}
	for _, j := range jobs {
		if s := internal.JobState(j.State.Load()); s != internal.StateFailed {
			t.Errorf("job %s state = %d, want failed", j.ID, s)
		}
	}
}
