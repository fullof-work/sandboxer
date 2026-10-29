#!/usr/bin/env python3
"""Test-only CH HTTP interposer: inspect complete RAM while CH is paused.

All bytes are read from the CH process VMA, so missing pages are resolved by
real UFFD, including inherited and released pages. Inspection occurs immediately
before forwarding resume, after capture has frozen and encoded its own M/D/R.
It never changes a production capture hook or artifact.
"""
import ctypes
import hashlib
import http.client
import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid

class IOVec(ctypes.Structure):
    _fields_ = [("base", ctypes.c_void_p), ("length", ctypes.c_size_t)]

def inspect_ram(pid, output, sequence):
    mappings = []
    with open(f"/proc/{pid}/maps") as source:
        for line in source:
            fields = line.split()
            if len(fields) < 6 or "memfd:sandbox-" not in fields[5] or "-ram" not in fields[5]:
                continue
            start, end = [int(v, 16) for v in fields[0].split("-")]
            mappings.append((int(fields[2], 16), start, end-start))
    mappings.sort()
    if not mappings:
        raise RuntimeError("CH RAM mappings not found")
    libc = ctypes.CDLL(None, use_errno=True)
    read = libc.process_vm_readv
    read.argtypes = [ctypes.c_int, ctypes.POINTER(IOVec), ctypes.c_ulong, ctypes.POINTER(IOVec), ctypes.c_ulong, ctypes.c_ulong]
    read.restype = ctypes.c_ssize_t
    buf = ctypes.create_string_buffer(1 << 20)
    offset = 0
    digest = hashlib.sha256()
    path = f"{output}/ram-{pid}-{sequence}.bin"
    with open(path + ".pending", "wb") as target:
        for file_offset, va, length in mappings:
            if file_offset != offset:
                raise RuntimeError(f"RAM registration gap/alias at {offset}: {mappings}")
            for pos in range(0, length, len(buf)):
                count = min(len(buf), length-pos)
                local = IOVec(ctypes.addressof(buf), count)
                remote = IOVec(va+pos, count)
                done = read(pid, ctypes.byref(local), 1, ctypes.byref(remote), 1, 0)
                if done != count:
                    raise OSError(ctypes.get_errno(), f"process_vm_readv {done}/{count} at {va+pos:x}")
                value = buf.raw[:count]
                digest.update(value)
                target.write(value)
                offset += count
    os.rename(path + ".pending", path)
    record = {"run_id": run_id, "pid": pid, "sequence": sequence, "bytes": offset, "sha256": digest.hexdigest(), "path": path, "mappings": mappings}
    with open(f"{output}/observations.jsonl", "a") as target:
        target.write(json.dumps(record) + "\n")
    print("dirty-memory RAM observation: " + json.dumps(record), file=sys.stderr, flush=True)

args = sys.argv[1:]
if "--version" in args:
    os.execv(os.environ["DIRTY_REAL_CH"], [os.environ["DIRTY_REAL_CH"]] + args)
index = args.index("--api-socket") + 1
front = args[index]
back = front + ".actual"
args[index] = back
inherited = []
for fd in range(3, 32):
    try:
        os.fstat(fd)
        inherited.append(fd)
    except OSError:
        pass
listener = socket.socket(socket.AF_UNIX)
listener.bind(front)
listener.listen(16)
def parent_death_signal():
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(1, signal.SIGKILL, 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), "PR_SET_PDEATHSIG")
child = subprocess.Popen([os.environ["DIRTY_REAL_CH"]] + args, pass_fds=tuple(inherited), preexec_fn=parent_death_signal)
sequence = 0
capture_sequence = 0
run_id = uuid.uuid4().hex
lock = threading.Lock()
output = os.environ["DIRTY_OBSERVATIONS"]
os.makedirs(output, exist_ok=False)

def handle(conn):
    global sequence, capture_sequence
    try:
        request = conn.makefile("rb")
        first = request.readline()
        if not first:
            return
        headers = []
        length = 0
        while True:
            line = request.readline()
            if line == b"\r\n":
                break
            if not line:
                raise RuntimeError("incomplete HTTP header")
            headers.append(line)
            if line.lower().startswith(b"content-length:"):
                length = int(line.split(b":", 1)[1])
        body = request.read(length)
        if len(body) != length:
            raise RuntimeError("incomplete HTTP body")
        if first.split()[1] == b"/api/v1/vm.resume":
            with lock:
                sequence += 1
                inspect_ram(child.pid, output, sequence)
        if first.split()[1] == b"/api/v1/vm.dirty-memory":
            with open(f"{output}/dirty-requests.jsonl", "a") as target:
                target.write(json.dumps({"pid": child.pid, "run_id": run_id}) + "\n")
            if os.environ.get("DIRTY_MEMORY_HIDE_LOG") == "1":
                conn.sendall(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                return
        backend = socket.socket(socket.AF_UNIX)
        for attempt in range(500):
            try:
                backend.connect(back)
                break
            except (FileNotFoundError, ConnectionRefusedError):
                time.sleep(0.01)
        # Test-only release injection after sandboxer has acquired its snapshot
        # balloon mutation gate and quiesced the app, before vCPUs are paused.
        # The ordinary memory controller cannot race this target. The real
        # guest balloon thread still runs; no response or RAM bytes are forged.
        # CaptureState's earlier diagnostic observation precedes this injection;
        # CH's subsequent snapshot contains the resulting actual device state.
        balloon_marker = f"{output}/balloon-before-pause"
        if first.split()[1] == b"/api/v1/vm.pause" and os.path.exists(balloon_marker):
            with lock:
                with open(balloon_marker) as source:
                    target = int(source.read())
                os.unlink(balloon_marker)
                with open(f"{output}/balloon-inflate.jsonl", "w") as report:
                    subprocess.run([sys.executable,
                                    os.path.join(os.environ["SANDBOXER_LIB"], "dirty_memory_balloon.py"),
                                    back, str(target)], stdout=report, check=True, timeout=20)
        backend.sendall(first + b"".join(headers) + b"\r\n" + body)
        response = http.client.HTTPResponse(backend)
        response.begin()
        value = response.read()
        if first.split()[1] == b"/api/v1/vm.dirty-memory" and response.status == 200:
            report = json.loads(value)
            # A fully resident map cannot grow further through clean tails.
            size = sum(region["size"] for region in report["regions"])
            holes = []
            cursor = 0
            while cursor < size:
                try:
                    data = os.lseek(3, cursor, os.SEEK_DATA)
                except OSError as error:
                    if error.errno != 6:
                        raise
                    data = size
                if data > cursor:
                    holes.append([cursor, data-cursor])
                if data >= size:
                    break
                cursor = os.lseek(3, data, os.SEEK_HOLE)
            with open(f"{output}/capture-residency.jsonl", "a") as target:
                capture_sequence += 1
                target.write(json.dumps({"run_id": run_id, "pid": child.pid, "capture_sequence": capture_sequence, "resume_sequence": sequence+1, "size": size, "holes": holes, "dirty": report}) + "\n")
        drop = f"{output}/drop-dirty-once"
        if first.split()[1] == b"/api/v1/vm.dirty-memory" and os.path.exists(drop):
            os.unlink(drop)
            with open(f"{output}/lost-dirty-response.json", "wb") as target:
                target.write(value)
            # CH consumed the kernel logs and produced its cumulative response;
            # inject response loss before sandboxer can consume any of it.
            backend.close()
            return
        conn.sendall(f"HTTP/1.1 {response.status} {response.reason}\r\nContent-Length: {len(value)}\r\nConnection: close\r\n\r\n".encode() + value)
        backend.close()
    except Exception as error:
        print(f"dirty-memory interposer failure: {error}", file=sys.stderr, flush=True)
        conn.sendall(b"HTTP/1.1 500 Inspection Failed\r\nContent-Length: 0\r\n\r\n")
    finally:
        conn.close()

def serve():
    while child.poll() is None:
        try:
            conn, _ = listener.accept()
        except OSError:
            return
        threading.Thread(target=handle, args=(conn,), daemon=True).start()
threading.Thread(target=serve, daemon=True).start()
for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
    signal.signal(sig, lambda number, frame: child.send_signal(number))
sys.exit(child.wait())
