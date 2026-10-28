package sandbox

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/sandboxer/pkg/proto"
)

func initialRuntimeForTest(t *testing.T) *Runtime {
	t.Helper()
	return &Runtime{state: runtimeLaunching, lifeCtx: context.Background(), launchContext: context.Background(),
		launch: completedLaunchServer(t), initialLaunchToken: "current", initialLaunchChanged: make(chan struct{}, 1), launchDone: make(chan struct{}),
		initialLaunchState: proto.InitialLaunchState{Token: "current", State: proto.InitialLaunchPending}}
}

func TestInitialLaunchFactValidationAndTerminalFence(t *testing.T) {
	for _, state := range []string{proto.InitialLaunchPending, proto.InitialLaunchStarted, proto.InitialLaunchFailed, proto.InitialLaunchExited, "running", "bad"} {
		t.Run(state, func(t *testing.T) {
			r := initialRuntimeForTest(t)
			r.observeInitialLaunch(proto.InitialLaunchState{Token: "old", State: proto.InitialLaunchStarted, PID: 999})
			if r.commitLaunch() {
				t.Fatal("stale token committed")
			}
			r.observeInitialLaunch(proto.InitialLaunchState{Token: "current", State: state, PID: 42, Code: 7, Error: "failed"})
			got := r.commitLaunch()
			if got != (state == proto.InitialLaunchStarted) {
				t.Fatalf("state=%s commit=%v", state, got)
			}
			if state != proto.InitialLaunchPending && state != proto.InitialLaunchStarted {
				if r.launchFailure == nil {
					t.Fatal("invalid/terminal fact left pending")
				}
				r.observeInitialLaunch(proto.InitialLaunchState{Token: "current", State: proto.InitialLaunchStarted, PID: 42})
				if r.commitLaunch() {
					t.Fatal("late started erased failure")
				}
			}
		})
	}
}

func TestInitialLaunchEventPongRaceCommitsOnce(t *testing.T) {
	r := initialRuntimeForTest(t)
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Go(func() {
			r.observeInitialLaunch(proto.InitialLaunchState{Token: "current", State: proto.InitialLaunchStarted, PID: 42})
			r.commitLaunch()
		})
	}
	wg.Wait()
	select {
	case <-r.launchDone:
	default:
		t.Fatal("internal completion missing")
	}
	if !r.launchSucceeded || r.state != runtimeRunning {
		t.Fatal("initial success was not retained")
	}
	r.observeInitialLaunch(proto.InitialLaunchState{Token: "current", State: proto.InitialLaunchExited})
	if !r.commitLaunch() {
		t.Fatal("later exit rewrote committed outcome")
	}
}

func TestInitialLaunchFailurePreventsQueuedReadinessCommit(t *testing.T) {
	for _, failure := range []string{"deadline", "terminal", "invalid", "owner"} {
		t.Run(failure, func(t *testing.T) {
			r := initialRuntimeForTest(t)
			r.observeInitialLaunch(proto.InitialLaunchState{Token: "current", State: proto.InitialLaunchStarted, PID: 42})
			switch failure {
			case "deadline":
				r.mu.Lock()
				r.launchFailure = errors.New("explicit deadline")
				r.mu.Unlock()
			case "terminal":
				r.observeInitialLaunch(proto.InitialLaunchState{Token: "current", State: proto.InitialLaunchExited, Code: 7})
			case "invalid":
				r.observeInitialLaunch(proto.InitialLaunchState{Token: "current", State: "running"})
			case "owner":
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				r.lifeCtx = ctx
			}
			if r.commitLaunch() {
				t.Fatal("queued readiness committed after failure")
			}
		})
	}
}

func TestSDKPongRecoversLostInitialNotificationAfterOldWindow(t *testing.T) {
	for _, budget := range []string{"", "0"} {
		t.Run("app_start="+budget, func(t *testing.T) {
			shape, launch := sdkFixture(t)
			shape.Timeouts.AppStart = budget
			gate := filepath.Join(shape.RuntimeRoot, "app-gate.sock")
			listener, err := net.Listen("unix", gate)
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			t.Setenv("SDK_TEST_APP_GATE", gate)
			t.Setenv("SDK_TEST_DROP_APP_STARTED", "1")
			ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
			defer cancel()
			r, err := StartRuntime(ctx, shape)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			result := make(chan error, 1)
			go func() { result <- r.Launch(ctx, launch) }()
			gateConn, err := listener.Accept()
			if err != nil {
				t.Fatal(err)
			}
			defer gateConn.Close()
			// A real child is parked after ACK; Pinger observes pending while
			// the same launch spans the former two-second overall deadline.
			<-r.launch.LaunchAckDone()
			r.mu.Lock()
			token := r.initialLaunchToken
			r.mu.Unlock()
			select {
			case err := <-result:
				t.Fatalf("pending launch ended: %v", err)
			case <-time.After(2100 * time.Millisecond):
			}
			if r.State() != "launching" {
				t.Fatal(r.State())
			}
			if _, err := gateConn.Write([]byte{1}); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-result:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("Pong did not complete same Launch")
			}
			r.mu.Lock()
			same := r.initialLaunchToken == token
			r.mu.Unlock()
			if !same || r.State() != "running" {
				t.Fatal("launch identity changed")
			}
			select {
			case <-r.launch.AppStartedDone():
				t.Fatal("fixture accidentally delivered app_started")
			default:
			}
		})
	}
}

func TestSDKRejectsOldGuestBeforeLaunchApply(t *testing.T) {
	t.Setenv("SDK_TEST_OLD_GUEST", "1")
	shape, _ := sdkFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	r, err := StartRuntime(ctx, shape)
	if r != nil {
		r.Close()
		t.Fatal("old Guest admitted")
	}
	if err == nil || !strings.Contains(err.Error(), "initial_launch_state_v1") {
		t.Fatalf("compatibility error=%v", err)
	}
}
