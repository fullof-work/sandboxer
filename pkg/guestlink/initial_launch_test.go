package guestlink

import (
	"bytes"
	"context"
	"io"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/sandboxer/pkg/proto"
)

func TestInitialLaunchPongCompensatesLostNotificationACKAndPreservesPauseEOF(t *testing.T) {
	fact := proto.InitialLaunchState{Token: "one-launch", State: proto.InitialLaunchStarted, PID: 42}
	facts := make(chan proto.InitialLaunchState, 2)
	publish := func(s proto.InitialLaunchState) { facts <- s }
	srv := &LaunchServer{Logf: t.Logf, OnInitialLaunchState: publish}
	conn := &scriptedLaunchConn{read: bytes.NewReader(encodeLaunchMessage(t, &proto.Message{
		Type: proto.TypeAppStarted, PID: 42, InitialLaunchState: &fact,
	})), write: func([]byte) (int, error) { return 0, io.ErrClosedPipe }}
	srv.handleConn(conn)
	select {
	case <-facts:
		t.Fatal("failed notification ACK published success")
	default:
	}
	base := filepath.Join(t.TempDir(), "vsock")
	written, allowEOF := make(chan struct{}), make(chan struct{})
	var closeOnce sync.Once
	releaseEOF := func() { closeOnce.Do(func() { close(allowEOF) }) }
	proxy := newFakeCHProxy(t, base, func(c net.Conn) {
		req, err := proto.ReadMessage(c)
		if err != nil {
			return
		}
		if err := proto.WriteMessage(c, &proto.Message{Type: proto.TypePong, ID: req.ID, InitialLaunchState: &fact}); err != nil {
			return
		}
		close(written)
		<-allowEOF
	})
	defer proxy.close()
	defer releaseEOF()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	p := &Pinger{Client: &HostClient{BasePath: base}, Cfg: PingerConfig{Interval: time.Hour, Timeout: 2 * time.Second},
		OnInitialLaunchState: publish, Logf: t.Logf}
	p.Start(ctx)
	defer p.Stop()
	<-written
	select {
	case <-facts:
		t.Fatal("Pong fact escaped before connection EOF")
	default:
	}
	paused := make(chan error, 1)
	drain := &pingDrainObservedContext{Context: ctx, observed: make(chan struct{})}
	go func() { paused <- p.PauseContext(drain) }()
	select {
	case <-drain.observed:
	case <-ctx.Done():
		t.Fatal("Pause did not enter the in-flight probe barrier")
	}
	releaseEOF()
	select {
	case err := <-paused:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("Pause did not join Pong EOF")
	}
	select {
	case got := <-facts:
		if got != fact {
			t.Fatalf("Pong changed fact: %+v", got)
		}
	default:
		t.Fatal("Pong did not compensate lost ACK")
	}
	p.Stop()
	select {
	case <-facts:
		t.Fatal("Pause/Stop admitted another fact")
	default:
	}
}
