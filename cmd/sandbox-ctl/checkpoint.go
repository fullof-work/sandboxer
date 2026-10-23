package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	"github.com/kuasar-sandbox/sandboxer/pkg/ctl"
	"github.com/kuasar-sandbox/sandboxer/pkg/diffstore"
)

func checkpointCmd(args []string) int {
	if len(args) > 0 && args[0] == "gc" {
		return checkpointGCCmd(args[1:])
	}
	if len(args) == 0 || (args[0] != "list" && args[0] != "delete") {
		fmt.Fprintln(os.Stderr, "usage: sandbox-ctl checkpoint list|delete (--sandbox-id ID | --checkpoint-dir DIR) [--checkpoint ID] [--diff-id ID ...] [--json]\n       sandbox-ctl checkpoint gc --checkpoint-dir DIR [--json]")
		return 2
	}
	action := args[0]
	fs := flag.NewFlagSet("checkpoint "+action, flag.ContinueOnError)
	sid := fs.String("sandbox-id", "", "running sandbox identity")
	pathID := fs.String("path-id", "", "run-root directory leaf")
	runRoot := fs.String("run-root", "", "runtime root (or SANDBOX_RUN_ROOT)")
	directory := fs.String("checkpoint-dir", "", "offline directory management target")
	id := fs.String("checkpoint", "", "checkpoint ID to delete")
	jsonOutput := fs.Bool("json", false, "emit exact disk/version references")
	var diffIDs repeatedFlag
	fs.Var(&diffIDs, "diff-id", "exact disk identity for retryable deletion (repeatable)")
	if err := fs.Parse(args[1:]); err != nil {
		return 2
	}
	if fs.NArg() != 0 || (*directory != "") == (*sid != "" || *pathID != "") {
		fmt.Fprintln(os.Stderr, "checkpoint: select exactly one running sandbox or offline directory")
		return 2
	}
	if (action == "delete" && *id == "") || (action == "list" && (*id != "" || len(diffIDs) != 0)) {
		fmt.Fprintln(os.Stderr, "checkpoint: delete requires --checkpoint; list accepts no deletion filter")
		return 2
	}
	ctx, stop := commandContext()
	defer stop()
	storage, err := artifact.NewProcessStorage(nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer storage.Close()
	socket := ""
	if *directory == "" {
		target, err := resolveTargetPathID(*sid, *pathID)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
		rd := *runRoot
		if rd == "" {
			rd = os.Getenv("SANDBOX_RUN_ROOT")
		}
		if rd == "" {
			rd = "/run/sandbox"
		}
		socket = filepath.Join(rd, target, "ctl.sock")
	}
	var refs []artifact.DiffCheckpointRef
	// Explicit DiffIDs bypass rediscovery and retry precisely the original set,
	// including versions already tombstoned by a partially completed deletion.
	if len(diffIDs) > 0 {
		for _, d := range diffIDs {
			refs = append(refs, artifact.DiffCheckpointRef{DiffID: d, ID: *id})
		}
	} else if socket != "" {
		var wire []ctl.DiffCheckpointRef
		wire, err = ctl.ListDiffCheckpoints(ctx, socket)
		for _, r := range wire {
			refs = append(refs, artifact.DiffCheckpointRef{DiffID: r.DiffID, ID: r.ID})
		}
	} else {
		refs, err = storage.ListDiffCheckpoints(ctx, *directory)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if action == "delete" {
		selected := make([]artifact.DiffCheckpointRef, 0, len(refs))
		for _, r := range refs {
			if r.ID == *id {
				selected = append(selected, r)
			}
		}
		refs = selected
		if len(refs) == 0 {
			fmt.Fprintln(os.Stderr, "checkpoint: no matching version")
			return 1
		}
		if socket != "" {
			wire := make([]ctl.DiffCheckpointRef, len(refs))
			for i, r := range refs {
				wire[i] = ctl.DiffCheckpointRef{DiffID: r.DiffID, ID: r.ID}
			}
			err = ctl.DeleteDiffCheckpoints(ctx, socket, "", wire)
		} else {
			for _, r := range refs {
				if e := storage.DeleteDiffCheckpoint(ctx, *directory, r); e != nil {
					err = errors.Join(err, e)
				}
			}
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "checkpoint deletion incomplete: %v; retry the following exact references using --diff-id\n", err)
			_ = json.NewEncoder(os.Stderr).Encode(refs)
			return 1
		}
	}
	if *jsonOutput {
		if refs == nil {
			refs = []artifact.DiffCheckpointRef{}
		}
		if err := json.NewEncoder(os.Stdout).Encode(refs); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
	} else {
		for _, r := range refs {
			fmt.Printf("%s\t%s\n", r.DiffID, r.ID)
		}
	}
	return 0
}

// Metadata reclamation needs no artifact storage, fetcher, or data key.
func checkpointGCCmd(args []string) int {
	fs := flag.NewFlagSet("checkpoint gc", flag.ContinueOnError)
	directory := fs.String("checkpoint-dir", "", "offline directory to reclaim")
	jsonOutput := fs.Bool("json", false, "emit per-DiffID state and committed freed_slots")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 0 || *directory == "" {
		fmt.Fprintln(os.Stderr, "usage: sandbox-ctl checkpoint gc --checkpoint-dir DIR [--json]")
		return 2
	}
	ctx, stop := commandContext()
	defer stop()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	owner, err := diffstore.LockDirectory(*directory)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	m, err := diffstore.NewDirectoryMaintenance(owner, nil)
	if err != nil {
		_ = owner.Close()
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	var outputErr error
	write := func(s string) {
		if outputErr != nil {
			return
		}
		_, outputErr = fmt.Fprint(os.Stdout, s)
		if outputErr != nil {
			cancel()
		}
	}
	if *jsonOutput {
		write("[")
	}
	first := true
	err = m.Sweep(ctx, func(result diffstore.DirectoryGCResult) {
		if *jsonOutput {
			if !first {
				write(",")
			}
			first = false
			b, e := json.Marshal(result)
			if e != nil {
				outputErr = e
				cancel()
				return
			}
			write(string(b))
		} else {
			write(fmt.Sprintf("%s\t%s\tfreed_slots=%d", result.DiffID, result.State, result.FreedSlots))
			if result.State == string(diffstore.BlockedByReaders) {
				write("\tpending reclamation: readers still hold deleted versions")
			}
			if result.Error != "" {
				write("\t" + result.Error)
			}
			write("\n")
		}
	})
	if *jsonOutput {
		write("]\n")
	}
	// Close the borrower before releasing directory ownership, including errors.
	err = errors.Join(err, outputErr, m.Close(), owner.Close())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}
