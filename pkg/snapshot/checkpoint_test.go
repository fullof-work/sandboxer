package snapshot

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/sandboxer/pkg/ctl"
)

type checkpointSink struct {
	captureSink
	absorbErr, commitErr error
}

func (s *checkpointSink) AbsorbSandbox(ctx context.Context, src sparse.Source) (string, string, error) {
	if s.absorbErr != nil {
		return "", "", s.absorbErr
	}
	return s.captureSink.AbsorbSandbox(ctx, src)
}
func (s *checkpointSink) CommitSandbox(ctx context.Context, ref, path string) error {
	_ = s.captureSink.CommitSandbox(ctx, ref, path)
	return s.commitErr
}

func TestNamedCaptureRetainsOnlyAtOperationRootBoundary(t *testing.T) {
	for _, tc := range []struct {
		name                          string
		failDisk                      int
		absorb, commit, close, resume bool
		wantDeleted                   int
	}{
		{"second-disk", 1, false, false, false, false, 1},
		{"pre-commit-absorb", -1, true, false, false, false, 2},
		{"uncertain-commit", -1, false, true, false, false, 0},
		{"post-commit-close", -1, false, false, true, false, 0},
		{"post-commit-resume", -1, false, false, false, true, 0},
		{"success", -1, false, false, false, false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sock := filepath.Join(t.TempDir(), "ch.sock")
			ln, err := net.Listen("unix", sock)
			if err != nil {
				t.Fatal(err)
			}
			srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.resume && strings.HasSuffix(r.URL.Path, "resume") {
					w.WriteHeader(500)
					return
				}
				w.WriteHeader(http.StatusNoContent)
			})}
			done := make(chan struct{})
			go func() { _ = srv.Serve(ln); close(done) }()
			t.Cleanup(func() { _ = srv.Close(); <-done })
			p, err := exportTestPortable(t).Checkpointed("manifest://" + strings.Repeat("f", 64))
			if err != nil {
				t.Fatal(err)
			}
			q := &recordingQuiescer{}
			ready, registered, deleted := 0, 0, 0
			disks := make([]DiskDiff, 2)
			for i := range disks {
				disks[i] = DiskDiff{DiffID: strings.Repeat(string(rune('a'+i)), 32),
					SnapshotView: func() (io.ReadSeeker, []sparse.Extent, error) {
						t.Error("named capture scanned payload")
						return nil, nil, errors.New("payload scan")
					},
					WaitReady: func(context.Context) error {
						if q.quiesce != 0 {
							t.Error("Ready waited after freeze")
						}
						ready++
						return nil
					},
					Checkpoint: func(context.Context, string) error {
						if ready != 2 || q.quiesce != 1 {
							t.Error("registration before Ready/freeze")
						}
						if i == tc.failDisk {
							return errors.New("disk failure")
						}
						registered++
						return nil
					},
					WaitMaterialized: func(context.Context) error {
						if q.quiesce != 1 {
							t.Error("Materialized waited before freeze")
						}
						return nil
					},
					DeleteCheckpoint: func(context.Context, string) error { deleted++; return nil },
				}
			}
			sink := &checkpointSink{}
			if tc.absorb {
				sink.absorbErr = errors.New("absorb")
			}
			if tc.commit {
				sink.commitErr = errors.New("uncertain commit")
			}
			if tc.close {
				sink.closeErr = errors.New("close")
			}
			_, err = Export(context.Background(), ExportSources{CheckpointID: "stable", SandboxID: "sid", APISock: sock, CHApiDeadline: time.Second, PortableConfig: p, Diffs: disks, Quiescer: q}, sink, true)
			if (err != nil) != (tc.name != "success") {
				t.Fatalf("error=%v", err)
			}
			if deleted != tc.wantDeleted {
				t.Fatalf("deleted=%d want=%d (registered=%d error=%v)", deleted, tc.wantDeleted, registered, err)
			}
		})
	}
}

func TestNamedCaptureUnsupportedBeforeReadyOrFreeze(t *testing.T) {
	p, err := exportTestPortable(t).Checkpointed("manifest://" + strings.Repeat("f", 64))
	if err != nil {
		t.Fatal(err)
	}
	q := &recordingQuiescer{}
	disks := make([]DiskDiff, 2)
	for i := range disks {
		disks[i] = DiskDiff{SnapshotView: func() (io.ReadSeeker, []sparse.Extent, error) { return bytes.NewReader(nil), nil, nil }, WaitReady: func(context.Context) error { t.Error("unsupported capture waited Ready"); return nil }}
	}
	_, err = Export(context.Background(), ExportSources{CheckpointID: "stable", SandboxID: "sid", APISock: "must-not-open", PortableConfig: p, Diffs: disks, Quiescer: q}, &checkpointSink{}, true)
	if !errors.Is(err, ctl.ErrCheckpointUnsupported) {
		t.Fatalf("error=%v", err)
	}
	if q.quiesce != 0 || q.resume != 0 {
		t.Fatalf("unsupported capture touched backends: %+v", q)
	}
}
