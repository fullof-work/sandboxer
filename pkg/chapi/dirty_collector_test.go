package chapi

import (
	"context"
	"fmt"
	"testing"
)

func TestDirtyCollectorFixesCapabilityWithoutErrorDowngrade(t *testing.T) {
	valid := `{"version":1,"page_shift":12,"regions":[{"slot":0,"gpa":0,"size":4096,"file_offset":0,"bitmap":[1]}]}`
	reply := func(status, body string) string {
		return fmt.Sprintf("HTTP/1.1 %s\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s", status, len(body), body)
	}
	for _, first := range []string{"legacy", "native", "truncated-404", "500", "unknown-version"} {
		t.Run(first, func(t *testing.T) {
			c := NewDirtyMemoryCollector(Client{}, 4096)
			response := reply("200 OK", valid)
			switch first {
			case "legacy":
				response = reply("404 Not Found", `{"error":"Not Found"}`)
			case "truncated-404":
				response = "HTTP/1.1 404 Not Found\r\nContent-Length: 5\r\n\r\nx"
			case "500":
				response = reply("500 Internal Server Error", "")
			case "unknown-version":
				response = reply("200 OK", `{"version":2}`)
			}
			c.client.Sock = dirtyServer(t, response)
			got, supported, err := c.Collect(context.Background())
			switch first {
			case "legacy":
				if err != nil || supported || got != nil {
					t.Fatalf("legacy: %v %t %v", got, supported, err)
				}
				c.client.Sock = "/no-such-ch-socket"
				if _, supported, err = c.Collect(context.Background()); err != nil || supported {
					t.Fatal("legacy was probed again", err)
				}
			case "native":
				if err != nil || !supported || got.Regions[0].Bitmap[0] != 1 {
					t.Fatalf("native: %v %t %v", got, supported, err)
				}
				c.client.Sock = dirtyServer(t, reply("404 Not Found", ""))
				if _, _, err = c.Collect(context.Background()); err == nil {
					t.Fatal("native downgraded after 404")
				}
				c.client.Sock = dirtyServer(t, reply("200 OK", valid))
				if _, supported, err = c.Collect(context.Background()); err != nil || !supported {
					t.Fatal("retry failed", err)
				}
			default:
				if err == nil {
					t.Fatal("failed response accepted")
				}
				c.client.Sock = dirtyServer(t, reply("200 OK", valid))
				if _, supported, err = c.Collect(context.Background()); err != nil || !supported {
					t.Fatal("error selected legacy or prevented retry", err)
				}
			}
		})
	}
}
