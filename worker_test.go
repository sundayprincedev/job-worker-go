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
	return &internal.Job{ID: generateJobId(p), Payload: p, Timeout: types.UniversalJobTimeout, CreatedAt: time.Now()}
}

// resetRegistry empties the package-global registry so a test sees only its own jobs.
func resetRegistry(t *testing.T) {
	t.Helper()
	clear := func() {
		JobRegistry.Range(func(k, _ any) bool { JobRegistry.Delete(k); return true })
	}
	clear()
	t.Cleanup(clear)
}

func registered(j *internal.Job) bool {
	v, ok := JobRegistry.Load(j.ID)
	return ok && v.(*internal.Job) == j
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
	resetRegistry(t)
	ref := unique("dup")
	path := writeFile(t, `[
		{"merchant_acc":"M","amount":1,"customer_ref":"`+ref+`"},
		{"merchant_acc":"M","amount":1,"customer_ref":"`+ref+`"}
	]`)
	in, err := ReadJobs(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(in.Fresh) != 1 || len(in.Existing) != 0 {
		t.Fatalf("fresh=%d existing=%d, want 1 and 0", len(in.Fresh), len(in.Existing))
	}
}

func TestReadJobsLimit(t *testing.T) {
	resetRegistry(t)
	in, err := ReadJobs("jobs.json", 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(in.Fresh) != 3 {
		t.Fatalf("got %d fresh jobs, want 3", len(in.Fresh))
	}
	n := 0
	JobRegistry.Range(func(_, _ any) bool { n++; return true })
	if n != 3 {
		t.Fatalf("registry holds %d jobs, want only the 3 requested", n)
	}
}

// The loaded flag from LoadOrStore, not the job's state, decides the path.
func TestReadJobsRoutesByLoadedFlag(t *testing.T) {
	resetRegistry(t)
	path := writeFile(t, `[{"merchant_acc":"M","amount":1,"customer_ref":"`+unique("route")+`"}]`)

	first, err := ReadJobs(path, 0)
	if err != nil || len(first.Fresh) != 1 || len(first.Existing) != 0 {
		t.Fatalf("first read: %v fresh=%d existing=%d", err, len(first.Fresh), len(first.Existing))
	}

	// Still Pending, but already registered: must go to the resolver, not the pool.
	second, err := ReadJobs(path, 0)
	if err != nil || len(second.Fresh) != 0 || len(second.Existing) != 1 {
		t.Fatalf("second read: %v fresh=%d existing=%d", err, len(second.Fresh), len(second.Existing))
	}
	if second.Existing[0] != first.Fresh[0] {
		t.Fatal("re-reading must return the registered job, not a new one")
	}
}

func TestConcurrentSubmissionsCreateOnce(t *testing.T) {
	resetRegistry(t)
	p := payload(unique("race"), 5)

	const callers = 16
	created := make(chan bool, callers)
	for range callers {
		go func() { _, loaded := LoadOrStore(p); created <- !loaded }()
	}
	var creators int
	for range callers {
		if <-created {
			creators++
		}
	}
	if creators != 1 {
		t.Fatalf("%d callers believed they created the job, want exactly 1", creators)
	}
}

func TestParseCount(t *testing.T) {
	for _, tc := range []struct {
		n    string
		want int
		err  bool
	}{
		{"all", 0, false}, {"2", 2, false}, {"99", 99, false},
		{"0", 0, true}, {"-1", 0, true}, {"abc", 0, true},
	} {
		got, err := parseCount(tc.n)
		if (err != nil) != tc.err || got != tc.want {
			t.Errorf("parseCount(%q) = %d, err=%v; want %d, err=%v", tc.n, got, err, tc.want, tc.err)
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

// ---- resolver ----

func testResolver(staleAfter time.Duration) *Resolver {
	r := NewResolver()
	r.StaleAfter = staleAfter
	r.PollEvery = 2 * time.Millisecond
	return r
}

func jobInState(ref string, s internal.JobState, idle time.Duration) *internal.Job {
	j := newJob(unique(ref))
	j.State.Store(int32(s))
	j.CreatedAt = time.Now().Add(-idle)
	j.UpdatedAt.Store(time.Now().Add(-idle).UnixNano())
	return j
}

func TestResolveAlreadyTerminal(t *testing.T) {
	for _, s := range []internal.JobState{internal.StateCompleted, internal.StateFailed, internal.StateCanceled} {
		j := jobInState("term", s, time.Hour)
		res := testResolver(time.Millisecond).Resolve(context.Background(), j)
		if res.To != s || res.Err != nil {
			t.Errorf("state %d: got To=%d Err=%v, want untouched", s, res.To, res.Err)
		}
	}
}

func TestResolvePendingNeverPickedUpFails(t *testing.T) {
	j := jobInState("pend", internal.StatePending, time.Hour)
	j.CreatedAt = time.Now().Add(-time.Hour)
	res := testResolver(20*time.Millisecond).Resolve(context.Background(), j)
	if res.To != internal.StateFailed || !errors.Is(res.Err, internal.ErrStaleJob) {
		t.Fatalf("got To=%d Err=%v, want failed with ErrStaleJob", res.To, res.Err)
	}
}

func TestResolvePendingWaitsForCreator(t *testing.T) {
	j := jobInState("pend-live", internal.StatePending, 0)
	time.AfterFunc(20*time.Millisecond, func() { j.Transition(internal.StateCompleted) })
	res := testResolver(2*time.Second).Resolve(context.Background(), j)
	if res.To != internal.StateCompleted || res.Err != nil {
		t.Fatalf("got To=%d Err=%v, want the creator's completion respected", res.To, res.Err)
	}
}

func TestResolveCommittedIsCompletedNotRerun(t *testing.T) {
	j := jobInState("commit", internal.StateCommitted, time.Hour)
	res := testResolver(time.Millisecond).Resolve(context.Background(), j)
	if res.To != internal.StateCompleted || res.Err != nil {
		t.Fatalf("got To=%d Err=%v, want completed with no error", res.To, res.Err)
	}
	if internal.JobState(j.State.Load()) != internal.StateCompleted {
		t.Fatal("job state must be completed")
	}
}

func TestResolveStaleRunningFails(t *testing.T) {
	j := jobInState("stale", internal.StateRunning, time.Hour)
	res := testResolver(50*time.Millisecond).Resolve(context.Background(), j)
	if res.To != internal.StateFailed || !errors.Is(res.Err, internal.ErrStaleJob) {
		t.Fatalf("got To=%d Err=%v, want failed with ErrStaleJob", res.To, res.Err)
	}
	// A worker that wakes up late must lose the race, not resurrect the job.
	if j.Transition(internal.StateCompleted) {
		t.Fatal("late worker must not be able to complete a job the resolver failed")
	}
}

func TestResolveWaitsForLiveWorker(t *testing.T) {
	j := jobInState("live", internal.StateRunning, 0)
	time.AfterFunc(20*time.Millisecond, func() { j.Transition(internal.StateCompleted) })

	res := testResolver(2*time.Second).Resolve(context.Background(), j)
	if res.To != internal.StateCompleted || res.Err != nil {
		t.Fatalf("got To=%d Err=%v, want the worker's completion respected", res.To, res.Err)
	}
}

func TestResolveStopsWaitingOnCancel(t *testing.T) {
	j := jobInState("cancel", internal.StateRunning, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	res := testResolver(time.Hour).Resolve(ctx, j)
	if !errors.Is(res.Err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want deadline exceeded", res.Err)
	}
	if internal.JobState(j.State.Load()) != internal.StateRunning {
		t.Fatal("an abandoned wait must not change the job")
	}
}

func register(t *testing.T, j *internal.Job) {
	t.Helper()
	if _, loaded := JobRegistry.LoadOrStore(j.ID, j); loaded {
		t.Fatalf("job %s was already registered", j.ID)
	}
}

func TestDispatchTwoWayFlowAndEjection(t *testing.T) {
	resetRegistry(t)
	setFailureRate(t, 0)

	fresh := newJob(unique("fresh"))
	stale := jobInState("leftover", internal.StateRunning, time.Hour)
	done := jobInState("done", internal.StateCompleted, time.Hour)
	for _, j := range []*internal.Job{fresh, stale, done} {
		register(t, j)
	}

	var resolved int
	res := Dispatch(context.Background(),
		Intake{Fresh: []*internal.Job{fresh}, Existing: []*internal.Job{stale, done}},
		testResolver(20*time.Millisecond),
		Observer{OnResolve: func(Resolution) { resolved++ }})

	if res.Fresh != 1 || len(res.Resolutions) != 2 || resolved != 2 || res.Swept != 0 {
		t.Fatalf("fresh=%d resolutions=%d callbacks=%d swept=%d, want 1/2/2/0", res.Fresh, len(res.Resolutions), resolved, res.Swept)
	}
	if internal.JobState(stale.State.Load()) != internal.StateFailed {
		t.Error("stale job must be failed by the resolver")
	}
	for _, j := range []*internal.Job{fresh, stale, done} {
		if s := internal.JobState(j.State.Load()); !isTerminal(s) {
			t.Errorf("job %s ended in state %d, want terminal", j.ID, s)
		}
		if registered(j) {
			t.Errorf("job %s is terminal but still in the registry", j.ID)
		}
	}
}

func TestDispatchSweepsLeftoversNobodySubmitted(t *testing.T) {
	resetRegistry(t)
	setFailureRate(t, 0)

	orphan := jobInState("orphan", internal.StateCommitted, time.Hour)
	register(t, orphan)

	res := Dispatch(context.Background(), Intake{}, testResolver(20*time.Millisecond), Observer{})
	if res.Swept != 1 || len(res.Resolutions) != 1 {
		t.Fatalf("swept=%d resolutions=%d, want 1/1", res.Swept, len(res.Resolutions))
	}
	if internal.JobState(orphan.State.Load()) != internal.StateCompleted || registered(orphan) {
		t.Fatal("swept committed job must be completed and ejected")
	}
}

func TestEjectOnlyWhenTerminalAndOnlyThatJob(t *testing.T) {
	resetRegistry(t)
	j := newJob(unique("eject"))
	register(t, j)

	if ejectIfTerminal(j) || !registered(j) {
		t.Fatal("a pending job must stay in the registry")
	}
	j.Transition(internal.StateRunning)
	if ejectIfTerminal(j) || !registered(j) {
		t.Fatal("a running job must stay in the registry")
	}

	j.Transition(internal.StateCompleted)
	other := &internal.Job{ID: j.ID} // same ID, different job
	other.State.Store(int32(internal.StateCompleted))
	if ejectIfTerminal(other) || !registered(j) {
		t.Fatal("ejecting a different job with the same ID must not evict the registered one")
	}
	if !ejectIfTerminal(j) || registered(j) {
		t.Fatal("a terminal job must be ejected")
	}
}

func TestPoolPathEjectsTerminalJobs(t *testing.T) {
	resetRegistry(t)
	setFailureRate(t, 1) // every job fails fast, so all end terminal
	jobs := []*internal.Job{newJob(unique("pe")), newJob(unique("pe"))}
	for _, j := range jobs {
		register(t, j)
	}
	if err := RunJobs(context.Background(), jobs); err == nil {
		t.Fatal("expected simulated failures")
	}
	for _, j := range jobs {
		if registered(j) {
			t.Errorf("job %s finished but was not ejected", j.ID)
		}
	}
}
