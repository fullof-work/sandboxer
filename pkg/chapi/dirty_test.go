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
