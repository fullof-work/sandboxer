// dirtymemory is the static guest workload for full-memory capture E2E.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		panic("expected serve, mutate, read, or check")
	}
	const command = "/tmp/dirty-memory.command"
	const response = "/tmp/dirty-memory.response"
	if os.Args[1] == "serve" {
		pages := make([]byte, 32<<20)
		for i := range pages {
			pages[i] = byte(i*17 + 9)
		}
		report := func() {
			sum := sha256.Sum256(pages)
			b, _ := json.Marshal(map[string]any{"sha256": hex.EncodeToString(sum[:]), "bytes": len(pages)})
			if err := os.WriteFile(response, b, 0600); err != nil {
				panic(err)
			}
		}
		report()
		for {
			b, err := os.ReadFile(command)
			if err == nil {
				switch string(b) {
				case "mutate":
					for i := 0; i < len(pages); i += 1024 * 1024 {
						pages[i] ^= 0x79
					}
				case "read":
					_ = sha256.Sum256(pages)
				case "release":
					pages = nil
					runtime.GC()
					debug.FreeOSMemory()
				case "reuse":
					pages = make([]byte, 32<<20)
					for i := range pages {
						pages[i] = byte(i*19 + 53)
					}
				default:
					panic("unknown command")
				}
				if err := os.Remove(command); err != nil {
					panic(err)
				}
				report()
			}
			runtime.KeepAlive(pages)
			time.Sleep(10 * time.Millisecond)
		}
	}
	if os.Args[1] != "check" {
		_ = os.Remove(response)
		if err := os.WriteFile(command, []byte(os.Args[1]), 0600); err != nil {
			panic(err)
		}
	}
	for i := 0; i < 3000; i++ {
		b, err := os.ReadFile(response)
		if err == nil {
			fmt.Println(string(b))
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	panic("guest command timeout")
}
