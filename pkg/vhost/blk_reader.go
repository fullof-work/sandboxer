package vhost

import (
	"context"

	"github.com/kuasar-sandbox/sandboxer/pkg/diffstore"
)

// BlockReader is the read-only block source accepted by the transport.
type BlockReader = diffstore.BlockReader

func readBlock(ctx context.Context, r BlockReader, p []byte, off int64) (int, error) {
	if ctx == nil {
		return r.ReadAt(p, off)
	}
	if cr, ok := r.(interface {
		ReadAtContext(context.Context, []byte, int64) (int, error)
	}); ok {
		return cr.ReadAtContext(ctx, p, off)
	}
	return r.ReadAt(p, off)
}
