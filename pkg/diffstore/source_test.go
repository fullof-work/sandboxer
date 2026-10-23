package diffstore

import (
	"context"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
)

type recoveryStream struct {
	fetch.Stream
	read func(context.Context, []byte) (int, error)
}

func (s recoveryStream) ReadAt(ctx context.Context, p []byte, _ uint64) (int, error) {
	return s.read(ctx, p)
}
func (recoveryStream) Close() error { return nil }
