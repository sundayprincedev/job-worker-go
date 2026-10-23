package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/sundayprincedev/job-worker-go/internal"
)

func main() {
	os.Exit(run())
}

func run() int {
	var (
		file     = flag.String("file", "jobs.json", "path to the jobs JSON file")
		count    = flag.String("n", "all", `number of jobs to run, or "all"`)
		failRate = flag.Float64("fail-rate", FailureRate, "probability (0..1) that the simulated processor declines a job")
		leftover = flag.Bool("inject-leftovers", false, "simulate jobs a crashed run left behind (stale running, committed, in-flight, completed) so the resolver has work")
		inject   = flag.Duration("inject-signal", 0, "send SIGINT to this process after the given delay (e.g. 60ms); 0 disables")
	)
	flag.Parse()

	if *failRate < 0 || *failRate > 1 {
		fmt.Fprintln(os.Stderr, "error: -fail-rate must be between 0 and 1")
		return 1
	}
	FailureRate = *failRate

	limit, err := parseCount(*count)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}

	intake, err := ReadJobs(*file, limit)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *inject > 0 {
		logf("signal", "will inject SIGINT in %s", *inject)
		time.AfterFunc(*inject, func() {
			logf("signal", "injecting SIGINT")
			_ = syscall.Kill(os.Getpid(), syscall.SIGINT)
		})
	}

	if *leftover {
		intake = injectLeftovers(intake)
	}

	logf("main", "loaded %d new job(s) -> worker pool, %d already in registry -> resolver (from %s)", len(intake.Fresh), len(intake.Existing), *file)
	start := time.Now()

	var ok, failed atomic.Int64
	res := Dispatch(ctx, intake, NewResolver(), Observer{
		OnResult: func(r internal.JobResult) {
			if r.Success {
				ok.Add(1)
				logf("result", "%s OK", r.JobID)
			} else {
				failed.Add(1)
				logf("result", "%s FAILED: %v", r.JobID, r.Err)
			}
		},
		OnError: func(e internal.JobError) {
			logf("error", "%s", e.Error())
		},
		OnResolve: func(r Resolution) {
			logf("resolve", "%s %s -> %s: %s", r.JobID, stateName(r.From), stateName(r.To), r.Reason)
		},
	})
	runErr := res.RunErr

	if ctx.Err() != nil {
		logf("signal", "received shutdown signal; pool drained")
	}
	logf("main", "done in %s: pool %d ok, %d failed, %d never started; resolver handled %d",
		time.Since(start).Round(time.Millisecond), ok.Load(), failed.Load(), len(intake.Fresh)-int(ok.Load())-int(failed.Load()), len(res.Resolutions))
	if res.Swept > 0 {
		logf("main", "swept %d extra job(s) out of the registry", res.Swept)
	}
	for _, j := range append(append([]*internal.Job{}, intake.Fresh...), intake.Existing...) {
		logf("state", "%s -> %s", j.ID, stateName(internal.JobState(j.State.Load())))
	}
	left := 0
	JobRegistry.Range(func(_, _ any) bool { left++; return true })
	logf("main", "%d job(s) left in registry", left)

	if runErr != nil {
		return 2
	}
	return 0
}

// parseCount turns the -n flag into a job limit; 0 means all.
func parseCount(n string) (int, error) {
	if n == "all" {
		return 0, nil
	}
	v, err := strconv.Atoi(n)
	if err != nil || v <= 0 {
		return 0, fmt.Errorf(`invalid -n %q: want a positive integer or "all"`, n)
	}
	return v, nil
}

func stateName(s internal.JobState) string {
	switch s {
	case internal.StatePending:
		return "pending"
	case internal.StateRunning:
		return "running"
	case internal.StateCommitted:
		return "committed"
	case internal.StateCompleted:
		return "completed"
	case internal.StateFailed:
		return "failed"
	case internal.StateCanceled:
		return "canceled"
	}
	return "unknown"
}

func logf(tag, format string, args ...any) {
	fmt.Printf("%s [%-6s] %s\n", time.Now().Format("15:04:05.000"), tag, fmt.Sprintf(format, args...))
}

// injectLeftovers turns the first few fresh jobs into registry leftovers from a
// crashed earlier run (rewritten into the states such a run could leave behind)
// and moves them to Existing, so the resolver path has work to do.
func injectLeftovers(in Intake) Intake {
	set := func(j *internal.Job, s internal.JobState, age time.Duration) {
		j.State.Store(int32(s))
		j.UpdatedAt.Store(time.Now().Add(-age).UnixNano())
	}
	n := min(4, len(in.Fresh))
	for i, j := range in.Fresh[:n] {
		switch i {
		case 0: // worker died mid-run: stale Running
			set(j, internal.StateRunning, time.Second)
			logf("inject", "%s left Running with no progress", j.ID)
		case 1: // crash window: ledger committed, never closed
			set(j, internal.StateCommitted, time.Second)
			logf("inject", "%s left Committed but never closed", j.ID)
		case 2: // a live worker still owns it and finishes shortly
			set(j, internal.StateRunning, 0)
			logf("inject", "%s Running under a live worker", j.ID)
			time.AfterFunc(30*time.Millisecond, func() { j.Transition(internal.StateCompleted) })
		case 3: // already done, not yet ejected
			set(j, internal.StateCompleted, time.Second)
			logf("inject", "%s already Completed", j.ID)
		}
	}
	in.Existing = append(in.Existing, in.Fresh[:n]...)
	in.Fresh = in.Fresh[n:]
	return in
}
