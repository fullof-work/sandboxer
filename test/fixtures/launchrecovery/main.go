// launchrecovery is a prepared test fixture, never a product control hook.
// "keys" maps a logical disk read through the canonical carrier parser to its
// physical manifest chunks; "guest" performs independently gated real reads.
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/codec"
	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
)

type extent struct{ start, end int64 }
type trackingReader struct {
	io.ReaderAt
	reads []extent
	armed bool
}

func (r *trackingReader) ReadAt(b []byte, off int64) (int, error) {
	if r.armed {
		r.reads = append(r.reads, extent{off, off + int64(len(b))})
	}
	return r.ReaderAt.ReadAt(b, off)
}
func keys(args []string) error {
	if len(args) != 4 {
		return fmt.Errorf("keys MANIFEST CARRIER OFFSET LENGTH")
	}
	off, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil || off < 0 {
		return fmt.Errorf("invalid offset")
	}
	length, err := strconv.ParseInt(args[3], 10, 64)
	if err != nil || length <= 0 || length > 65536 {
		return fmt.Errorf("invalid length")
	}
	b, err := os.ReadFile(args[0])
	if err != nil {
		return err
	}
	m, _, err := codec.Unmarshal(b)
	if err != nil {
		return err
	}
	f, err := os.Open(args[1])
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	r := &trackingReader{ReaderAt: f}
	src, _, err := tarstream.SourceAt(r, st.Size(), "")
	if err != nil {
		return err
	}
	r.armed = true
	if _, err = src.ReadAt(context.Background(), make([]byte, int(length)), uint64(off)); err != nil {
		return err
	}
	var selected []string
	for _, chunk := range m.Entries {
		if chunk.IsZero {
			continue
		}
		for _, x := range r.reads {
			if int64(chunk.Offset) < x.end && int64(chunk.Offset)+int64(chunk.Size) > x.start {
				selected = append(selected, fmt.Sprintf("%x", chunk.CiphertextHash))
				break
			}
		}
	}
	if len(selected) == 0 {
		return fmt.Errorf("logical read selected no stored chunks")
	}
	fmt.Println(strings.Join(selected, ","))
	return nil
}
func guest() error {
	fmt.Println("LAUNCH-APP-STARTED")
	for {
		if _, err := os.Stat("/read.begin"); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	f, err := os.Open("/data/payload")
	if err != nil {
		return err
	}
	defer f.Close()
	b := make([]byte, 4096)
	if _, err := f.ReadAt(b, 1<<20); err != nil {
		return err
	}
	if !bytes.Equal(b, bytes.Repeat([]byte{5}, len(b))) {
		return fmt.Errorf("recovered read returned incorrect bytes")
	}
	fmt.Println("LAUNCH-READ-RECOVERED")
	for {
		time.Sleep(time.Hour)
	}
}
func main() {
	var err error
	if len(os.Args) > 1 && os.Args[1] == "guest" {
		err = guest()
	} else if len(os.Args) > 1 && os.Args[1] == "keys" {
		err = keys(os.Args[2:])
	} else {
		err = fmt.Errorf("expected keys or guest")
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
