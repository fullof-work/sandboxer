package sandbox

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// The ACK precedes the guest's process/cgroup/namespace bootstrap. A valid
// app_started event may arrive later than one launch-port message deadline.
func TestSDKAppStartWaitIsNotSocketDeadline(t *testing.T) {
	for _, socketDeadline := range []string{"", "200ms"} {
		t.Run("app_notify="+socketDeadline, func(t *testing.T) {
			t.Setenv("SDK_TEST_APP_STARTED_DELAY", "600ms")
			shape, launch := sdkFixture(t)
			shape.Timeouts.AppNotify = socketDeadline
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			r, err := StartRuntime(ctx, shape)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			if err := r.Launch(ctx, launch); err != nil {
				t.Fatalf("valid post-ACK process bootstrap rejected: %v", err)
			}
			if r.State() != "running" {
				t.Fatalf("state=%s", r.State())
			}
		})
	}
}

func TestSDKAppStartDeadlineAndCancellationCloseRuntime(t *testing.T) {
	for _, cancelOperation := range []bool{false, true} {
		name := "startup_deadline"
		if cancelOperation {
			name = "operation_deadline"
		}
		t.Run(name, func(t *testing.T) {
			t.Setenv("SDK_TEST_APP_STARTED_DELAY", "10s")
			shape, launch := sdkFixture(t)
			shape.Timeouts.AppNotify = "1h"
			shape.Timeouts.AppStart = "150ms"
			if cancelOperation {
				shape.Timeouts.AppStart = "5s"
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			r, err := StartRuntime(ctx, shape)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			if cancelOperation {
				// The operation must win even when the startup budget is longer.
				var cancelLaunch context.CancelFunc
				ctx, cancelLaunch = context.WithTimeout(ctx, 100*time.Millisecond)
				defer cancelLaunch()
			}
			started := time.Now()
			err = r.Launch(ctx, launch)
			if cancelOperation {
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("cancellation: %v", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), "initial launch timed out after 150ms") || ctx.Err() != nil {
				t.Fatalf("startup budget: %v, context=%v", err, ctx.Err())
			}
			if time.Since(started) > 2*time.Second {
				t.Fatal("failed launch cleanup exceeded bound")
			}
			if r.State() != "closed" {
				t.Fatalf("state=%s", r.State())
			}
			select {
			case <-r.exitDone:
			default:
				t.Fatal("failed launch retained the child process")
			}
		})
	}
}

func TestSDKAppStartDoesNotTimeoutCommittedReadinessCallback(t *testing.T) {
	for _, startupBudget := range []string{"", "100ms"} {
		t.Run("app_start="+startupBudget, func(t *testing.T) {
			shape, launch := sdkFixture(t)
			shape.Timeouts.AppNotify = "5s"
			shape.Timeouts.AppStart = startupBudget
			entered, release := make(chan struct{}), make(chan struct{})
			shape.NotifyReadiness = func(event ReadinessEvent) {
				if event == ReadinessReady {
					close(entered)
					<-release
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			r, err := StartRuntime(ctx, shape)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			// Release the callback before joining runtime cleanup, including on
			// a failed assertion. Launch must succeed while it is still blocked.
			defer close(release)
			result := make(chan error, 1)
			go func() { result <- r.Launch(ctx, launch) }()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("readiness callback was not entered")
			}
			select {
			case err := <-result:
				if err != nil {
					t.Fatalf("committed launch rejected for a blocked callback: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatalf("committed launch waited for callback: state=%s", r.State())
			}
			if r.State() != "running" {
				t.Fatalf("committed launch state=%s", r.State())
			}
			closed := make(chan error, 1)
			go func() { closed <- r.Close() }()
			select {
			case err := <-closed:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("Close/Pinger.Stop waited for blocked user callback")
			}
		})
	}
}

// Hold the real readiness worker immediately before its commit gate while the
// production post-ACK timer expires. Cleanup must freeze failure before the
// queued worker is released; it cannot resurrect the closed Runtime.
func TestSDKAppStartTimerFencesQueuedConfirmation(t *testing.T) {
	shape, launch := sdkFixture(t)
	shape.Timeouts.AppStart = "80ms"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r, err := StartRuntime(ctx, shape)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	original := r.notifyInitialReady
	queued, release, workerDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	r.notifyInitialReady = func() {
		close(queued)
		<-release
		original()
		close(workerDone)
	}
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	result := make(chan error, 1)
	go func() { result <- r.Launch(ctx, launch) }()
	select {
	case <-queued:
	case <-ctx.Done():
		t.Fatal("confirmation was not queued")
	}
	select {
	case err := <-result:
		if err == nil || !strings.Contains(err.Error(), "initial launch timed out after 80ms") {
			t.Fatalf("timer outcome: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timer failed to terminate blocked confirmation")
	}
	if r.State() != "closed" {
		t.Fatal(r.State())
	}
	// Check before deferred release as well: cleanup retained a definitive
	// failure despite having already received a valid started fact.
	r.mu.Lock()
	succeeded, failure := r.launchSucceeded, r.launchFailure
	r.mu.Unlock()
	if succeeded || failure == nil {
		t.Fatalf("success=%v failure=%v", succeeded, failure)
	}
	// Join the queued worker after the production timer has frozen failure.
	unblock()
	select {
	case <-workerDone:
	case <-ctx.Done():
		t.Fatal("late confirmation worker did not finish")
	}
	if r.commitLaunch() {
		t.Fatal("late worker rewrote expired Launch")
	}
}

func TestSDKReadinessCallbackCanCloseRuntime(t *testing.T) {
	shape, launch := sdkFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	allowClose := make(chan struct{})
	closed := make(chan error, 1)
	var r *Runtime
	shape.NotifyReadiness = func(event ReadinessEvent) {
		if event == ReadinessReady {
			<-allowClose
			closed <- r.Close()
		}
	}
	var err error
	r, err = StartRuntime(ctx, shape)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	// Publish r before the callback may use it; StartRuntime emits only
	// control readiness before the workload is installed.
	close(allowClose)
	if err := r.Launch(ctx, launch); err != nil {
		t.Fatalf("committed launch: %v", err)
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("callback Close deadlocked on management/Pinger/Launch join")
	}
}
