// Package diffstore provides writable disks with a shared bounded plaintext
// cache and single-layer raw/v1 or checkpoint-capable v2 storage. OpenDisk borrows
// its immutable base; callers close bases separately and close all disks before
// a caller-owned shared Cache. Checkpoints reports the optional live capability.
// Active views require a quiescent, open Disk; named views own independent pins.
//
// Store and its constructors expose v2 offline import/administration separately
// from the live Disk boundary. Do not open a second Store owner for a live Disk.
package diffstore
