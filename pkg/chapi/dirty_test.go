package chapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func dirtyServer(t *testing.T, response string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ch.sock")
	l, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go func() {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		var b [1024]byte
		_, _ = conn.Read(b[:])
		for len(response) > 0 {
			n := min(len(response), 173)
			if _, err := conn.Write([]byte(response[:n])); err != nil {
				return
			}
			response = response[n:]
		}
	}()
	return path
}
func TestDirtyMemoryReadsCompleteResponseAndValidatesRegions(t *testing.T) {
	const size = 64 * 4096 * 1024
	makeReport := func() DirtyMemory {
		return DirtyMemory{Version: 1, PageShift: 12, Regions: []DirtyRegion{{Slot: 3, GPA: 1 << 32, Size: size, FileOffset: 0, Bitmap: make([]uint64, 1024)}}}
	}
	for _, name := range []string{"valid", "truncated", "bad-version", "gap", "overlap", "bad-length", "extra-json", "unused", "non-200"} {
		t.Run(name, func(t *testing.T) {
			d := makeReport()
			d.Regions[0].Bitmap[1023] = 1 << 63
			switch name {
			case "bad-version":
				d.Version = 2
			case "gap":
				d.Regions[0].FileOffset = 4096
			case "overlap":
				d.Regions = append(d.Regions, d.Regions[0])
			case "bad-length":
				d.Regions[0].Bitmap = d.Regions[0].Bitmap[:1023]
			case "unused":
				d.Regions[0].Size -= 4096
			}
			body, _ := json.Marshal(d)
			if name == "extra-json" {
				body = append(body, []byte("{}")...)
			}
			status := "200 OK"
			if name == "non-200" {
				status = "500 Internal Server Error"
			}
			response := fmt.Sprintf("HTTP/1.1 %s\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s", status, len(body), body)
			if name == "truncated" {
				response = response[:len(response)-7]
			}
			report, err := (Client{Sock: dirtyServer(t, response), RespDeadline: time.Second}).DirtyMemory(context.Background(), size)
			if name != "valid" {
				if err == nil {
					t.Fatal("invalid response accepted")
				}
				return
			}
			if err != nil || report.Regions[0].Bitmap[1023] != 1<<63 {
				t.Fatalf("complete response: %v %v", report, err)
			}
		})
	}
	sock := dirtyServer(t, "HTTP/1.1 200 OK\r\nContent-Length: 999999999\r\n\r\n")
	if _, err := (Client{Sock: sock}).DirtyMemory(context.Background(), 4096); err == nil || !strings.Contains(err.Error(), "budget") {
		t.Fatalf("unbounded response: %v", err)
	}
}
func TestDirtyMemoryMapsSplitGPAWithUnalignedWordOffsets(t *testing.T) {
	d := DirtyMemory{Version: 1, PageShift: 12, Regions: []DirtyRegion{{Slot: 7, GPA: 1 << 32, FileOffset: 4096, Size: 4096, Bitmap: []uint64{1}}, {Slot: 3, GPA: 0, FileOffset: 0, Size: 4096, Bitmap: []uint64{0}}}}
	if err := d.validate(8192); err != nil {
		t.Fatal(err)
	}
	if d.Regions[0].Slot != 3 || d.Regions[1].FileOffset != 4096 {
		t.Fatal("lost region offsets")
	}
}

func TestDirtyMemoryHTTPBudgets(t *testing.T) {
	const body = `{"version":1,"page_shift":12,"regions":[{"slot":0,"gpa":0,"size":4096,"file_offset":0,"bitmap":[0]}]}`
	const bodyLimit = 21 + (1 << 20)
	const header = "HTTP/1.1 200 OK\r\nConnection: close\r\nX-Pad: "
	const end = "\r\n\r\n"
	wire := func(headerSize int, b string) string {
		return header + strings.Repeat("a", headerSize-len(header)-len(end)) + end + b
	}
	padded := body + strings.Repeat(" ", bodyLimit-len(body))
	for _, tc := range []struct {
		name, response string
		valid          bool
	}{
		{"exact-header-and-body", wire(dirtyHeaderBudget, padded), true},
		{"connection-close", wire(len(header)+len(end), body), true},
		{"header-plus-one", wire(dirtyHeaderBudget+1, body), false},
		{"body-plus-one", wire(dirtyHeaderBudget, padded+" "), false},
		{"hidden-second-json", wire(bodyLimit+dirtyHeaderBudget-len(body), body+"{}"), false},
		{"trailing-json", wire(len(header)+len(end), body+"{}"), false},
		{"trailing-invalid", wire(len(header)+len(end), body+"x"), false},
		{"truncated-length", fmt.Sprintf("HTTP/1.1 200 OK\r\nContent-Length: %d\r\n\r\n%s", len(body)+1, body), false},
		{"chunked", fmt.Sprintf("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n%x\r\n%s\r\n0\r\n\r\n", len(body), body), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := (Client{Sock: dirtyServer(t, tc.response), RespDeadline: 5 * time.Second}).DirtyMemory(context.Background(), 4096)
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v: %v", tc.valid, err)
			}
		})
	}
}
