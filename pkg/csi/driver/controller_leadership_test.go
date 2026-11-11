package driver

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestControllerLeaderInterceptorAllowsNonControllerMethods(t *testing.T) {
	leadership := &ControllerLeadership{enabled: true}
	interceptor := controllerLeaderInterceptor(leadership)

	called := false
	_, err := interceptor(context.Background(), "req", &grpc.UnaryServerInfo{
		FullMethod: "/csi.v1.Node/NodeGetInfo",
	}, func(ctx context.Context, req any) (any, error) {
		called = true
		return "ok", nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("expected non-controller method to reach handler")
	}
}

func TestControllerLeaderInterceptorRejectsFollowerControllerRPCs(t *testing.T) {
	leadership := &ControllerLeadership{
		enabled:        true,
		leaseName:      "lease-a",
		leaseNamespace: "kube-system",
	}
	interceptor := controllerLeaderInterceptor(leadership)

	called := false
	_, err := interceptor(context.Background(), "req", &grpc.UnaryServerInfo{
		FullMethod: "/csi.v1.Controller/ControllerPublishVolume",
	}, func(ctx context.Context, req any) (any, error) {
		called = true
		return "ok", nil
	})

	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable, got %v", err)
	}
	if called {
		t.Fatal("expected follower controller RPC to be rejected before handler")
	}
}

func TestControllerLeaderInterceptorAllowsCapabilitiesOnFollower(t *testing.T) {
	leadership := &ControllerLeadership{
		enabled:        true,
		leaseName:      "lease-a",
		leaseNamespace: "kube-system",
	}
	interceptor := controllerLeaderInterceptor(leadership)

	called := false
	_, err := interceptor(context.Background(), "req", &grpc.UnaryServerInfo{
		FullMethod: "/csi.v1.Controller/ControllerGetCapabilities",
	}, func(ctx context.Context, req any) (any, error) {
		called = true
		return "ok", nil
	})

	if err != nil {
		t.Fatalf("expected capabilities to be served by a follower, got %v", err)
	}
	if !called {
		t.Fatal("expected ControllerGetCapabilities to reach handler while not leader")
	}
}

func TestControllerLeaderInterceptorRejectsMutatingRPCsOnFollower(t *testing.T) {
	leadership := &ControllerLeadership{
		enabled:        true,
		leaseName:      "lease-a",
		leaseNamespace: "kube-system",
	}
	interceptor := controllerLeaderInterceptor(leadership)

	for _, method := range []string{
		"/csi.v1.Controller/CreateVolume",
		"/csi.v1.Controller/DeleteVolume",
		"/csi.v1.Controller/ControllerPublishVolume",
		"/csi.v1.Controller/ControllerUnpublishVolume",
		"/csi.v1.Controller/ControllerExpandVolume",
		"/csi.v1.Controller/CreateSnapshot",
		"/csi.v1.Controller/DeleteSnapshot",
	} {
		t.Run(method, func(t *testing.T) {
			called := false
			_, err := interceptor(context.Background(), "req", &grpc.UnaryServerInfo{
				FullMethod: method,
			}, func(ctx context.Context, req any) (any, error) {
				called = true
				return "ok", nil
			})
			if status.Code(err) != codes.Unavailable {
				t.Fatalf("expected Unavailable for %s, got %v", method, err)
			}
			if called {
				t.Fatalf("expected %s to be rejected before handler", method)
			}
		})
	}
}

func TestControllerLeaderInterceptorAllowsLeaderControllerRPCs(t *testing.T) {
	leadership := &ControllerLeadership{enabled: true}
	leadership.isLeader.Store(true)
	interceptor := controllerLeaderInterceptor(leadership)

	called := false
	_, err := interceptor(context.Background(), "req", &grpc.UnaryServerInfo{
		FullMethod: "/csi.v1.Controller/ControllerPublishVolume",
	}, func(ctx context.Context, req any) (any, error) {
		called = true
		return "ok", nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("expected leader controller RPC to reach handler")
	}
}

func TestRunControllerLeaderElectionLoopRestartsRunnerUntilContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var runs atomic.Int32
	done := make(chan struct{})

	go func() {
		runControllerLeaderElectionLoop(ctx, time.Millisecond, func(context.Context) {
			if runs.Add(1) == 2 {
				cancel()
			}
		})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for leader election loop to exit")
	}

	if got := runs.Load(); got < 2 {
		t.Fatalf("expected loop to restart runner at least once, got %d runs", got)
	}
}

func TestRunControllerLeaderElectionLoopStopsWhenContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var runs atomic.Int32
	started := make(chan struct{})
	done := make(chan struct{})

	go func() {
		runControllerLeaderElectionLoop(ctx, time.Millisecond, func(ctx context.Context) {
			runs.Add(1)
			select {
			case <-started:
			default:
				close(started)
			}
			<-ctx.Done()
		})
		close(done)
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for runner start")
	}

	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for leader election loop to stop")
	}

	if got := runs.Load(); got != 1 {
		t.Fatalf("expected runner to execute once before shutdown, got %d", got)
	}
}
