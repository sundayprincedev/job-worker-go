package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
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
		inject   = flag.Duration("inject-signal", 0, "send SIGINT to this process after the given delay (e.g. 60ms); 0 disables")
	)
	flag.Parse()

	if *failRate < 0 || *failRate > 1 {
		fmt.Fprintln(os.Stderr, "error: -fail-rate must be between 0 and 1")
		return 1
	}
	FailureRate = *failRate

	jobs, err := ReadJobs(*file)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}

	jobs, err = selectJobs(jobs, *count)
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

	logf("main", "running %d job(s) from %s", len(jobs), *file)
	start := time.Now()

	var ok, failed int
	runErr := RunJobsObserved(ctx, jobs, Observer{
		OnResult: func(r internal.JobResult) {
			if r.Success {
				ok++
				logf("result", "%s OK", r.JobID)
			} else {
				failed++
				logf("result", "%s FAILED: %v", r.JobID, r.Err)
			}
		},
		OnError: func(e internal.JobError) {
			logf("error", "%s", e.Error())
		},
	})

	if ctx.Err() != nil {
		logf("signal", "received shutdown signal; pool drained")
	}
	logf("main", "done in %s: %d ok, %d failed, %d never started",
		time.Since(start).Round(time.Millisecond), ok, failed, len(jobs)-ok-failed)
	for _, j := range jobs {
		logf("state", "%s -> %s", j.ID, stateName(internal.JobState(j.State.Load())))
	}

	if runErr != nil {
		return 2
	}
	return 0
}

func selectJobs(jobs []*internal.Job, n string) ([]*internal.Job, error) {
	if n == "all" {
		return jobs, nil
	}
	v, err := strconv.Atoi(n)
	if err != nil || v <= 0 {
		return nil, fmt.Errorf(`invalid -n %q: want a positive integer or "all"`, n)
	}
	if v > len(jobs) {
		v = len(jobs)
	}
	return jobs[:v], nil
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
