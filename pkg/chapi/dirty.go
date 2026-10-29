package chapi

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"time"
)

var ErrDirtyMemoryUnsupported = errors.New("dirty memory API is unavailable")

// DirtyRegion identifies a KVM slot and its mapping into the shared RAM memfd.
// Bitmap words are little-bit-first 4 KiB pages relative to GPA, not memfd zero.
type DirtyRegion struct {
	Slot       uint32   `json:"slot"`
	GPA        uint64   `json:"gpa"`
	Size       uint64   `json:"size"`
	FileOffset uint64   `json:"file_offset"`
	Bitmap     []uint64 `json:"bitmap"`
}
type DirtyMemory struct {
	Version   uint32        `json:"version"`
	PageShift uint32        `json:"page_shift"`
	Regions   []DirtyRegion `json:"regions"`
}

// DirtyMemory returns CH's lifetime accumulator. CH transfers each consumed
// slot before responding, so a lost, truncated or rejected response is safely
// retryable; it is never treated as an empty log.
func (c Client) DirtyMemory(ctx context.Context, memorySize uint64) (*DirtyMemory, error) {
	if memorySize == 0 || memorySize%4096 != 0 {
		return nil, fmt.Errorf("dirty memory: invalid RAM size")
	}
	// At most 21 JSON bytes per bitmap word, plus bounded region metadata. The
	// protocol has an independent HTTP budget; no single-read 4 KiB shortcut.
	const maxBody = 256 << 20
	words := (memorySize/4096 + 63) / 64
	if words > (maxBody-(1<<20))/21 {
		return nil, fmt.Errorf("dirty memory: response exceeds budget")
	}
	limit := int64(words*21 + (1 << 20))
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	conn, err := (&net.Dialer{}).DialContext(dialCtx, "unix", c.Sock)
	cancel()
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if c.RespDeadline > 0 {
		_ = conn.SetDeadline(time.Now().Add(c.RespDeadline))
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.SetDeadline(time.Now()) })
	defer stop()
	const path = "/api/v1/vm.dirty-memory"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://ch"+path, nil)
	if err != nil {
		return nil, err
	}
	req.Close = true
	if err = req.Write(conn); err != nil {
		return nil, fmt.Errorf("dirty memory request: %w", err)
	}
	response, err := readDirtyResponse(conn, req, limit)
	if err != nil {
		return nil, fmt.Errorf("dirty memory response: %w", err)
	}
	defer response.Body.Close()
	if response.ContentLength > limit {
		return nil, fmt.Errorf("dirty memory: response length exceeds budget")
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if err != nil {
		return nil, fmt.Errorf("dirty memory body: %w", err)
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("dirty memory: body exceeds budget")
	}
	if response.StatusCode == http.StatusNotFound {
		return nil, ErrDirtyMemoryUnsupported
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("dirty memory: HTTP status %s", response.Status)
	}
	var result DirtyMemory
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&result); err != nil {
		return nil, fmt.Errorf("dirty memory JSON: %w", err)
	}
	if err = decoder.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("dirty memory: trailing response data")
	}
	if err = result.validate(memorySize); err != nil {
		return nil, err
	}
	return &result, nil
}

const dirtyHeaderBudget = 64 << 10

// A limit is not a transport EOF. Probe one byte only when the budget is
// exhausted so an exactly bounded connection-delimited response still works.
type dirtyWireReader struct {
	r         io.Reader
	remaining int64
}

func (r *dirtyWireReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.remaining == 0 {
		var extra [1]byte
		n, err := r.r.Read(extra[:])
		if n != 0 {
			return 0, fmt.Errorf("dirty memory: wire exceeds budget")
		}
		return 0, err
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.r.Read(p)
	r.remaining -= int64(n)
	return n, err
}

func readDirtyResponse(conn io.Reader, req *http.Request, bodyBudget int64) (*http.Response, error) {
	reader := bufio.NewReaderSize(&dirtyWireReader{r: conn, remaining: bodyBudget + dirtyHeaderBudget}, 4096)
	// Bound headers independently, including the status line and final CRLF.
	// ReadSlice also bounds an unterminated line; retain body bytes prefetched
	// by this reader and replay only the validated header to net/http.
	var header []byte
	lineBytes := 0
	for {
		line, err := reader.ReadSlice('\n')
		if len(line) > dirtyHeaderBudget-len(header) {
			return nil, fmt.Errorf("dirty memory: headers exceed budget")
		}
		header = append(header, line...)
		lineBytes += len(line)
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if lineBytes == 2 && len(line) == 2 && line[0] == '\r' {
			break
		}
		lineBytes = 0
	}
	return http.ReadResponse(bufio.NewReader(io.MultiReader(bytes.NewReader(header), reader)), req)
}

func (d *DirtyMemory) validate(memorySize uint64) error {
	if d.Version != 1 || d.PageShift != 12 || len(d.Regions) == 0 || len(d.Regions) > 1024 {
		return fmt.Errorf("dirty memory: unsupported encoding or region count")
	}
	seen := make(map[uint32]bool, len(d.Regions))
	byGPA := append([]DirtyRegion(nil), d.Regions...)
	for _, r := range d.Regions {
		if seen[r.Slot] || r.Size == 0 || (r.GPA|r.Size|r.FileOffset)%4096 != 0 || r.GPA > ^uint64(0)-r.Size || r.FileOffset > memorySize || r.Size > memorySize-r.FileOffset || uint64(len(r.Bitmap)) != (r.Size/4096+63)/64 {
			return fmt.Errorf("dirty memory: invalid slot %d geometry/bitmap", r.Slot)
		}
		seen[r.Slot] = true
		if rem := r.Size / 4096 % 64; rem != 0 && r.Bitmap[len(r.Bitmap)-1]>>rem != 0 {
			return fmt.Errorf("dirty memory: nonzero unused bits")
		}
	}
	sort.Slice(byGPA, func(i, j int) bool { return byGPA[i].GPA < byGPA[j].GPA })
	for i := 1; i < len(byGPA); i++ {
		if byGPA[i].GPA < byGPA[i-1].GPA+byGPA[i-1].Size {
			return fmt.Errorf("dirty memory: overlapping GPA slots")
		}
	}
	sort.Slice(d.Regions, func(i, j int) bool { return d.Regions[i].FileOffset < d.Regions[j].FileOffset })
	var end uint64
	for _, r := range d.Regions {
		if r.FileOffset != end {
			return fmt.Errorf("dirty memory: RAM registration gap/overlap")
		}
		end += r.Size
	}
	if end != memorySize {
		return fmt.Errorf("dirty memory: incomplete RAM registration")
	}
	return nil
}
