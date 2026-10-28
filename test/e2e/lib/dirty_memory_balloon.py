#!/usr/bin/env python3
"""Resize the dedicated test VM's balloon and await its reported actual size."""
import http.client
import json
import socket
import sys
import time

path, target = sys.argv[1], int(sys.argv[2])


def call(method, endpoint, body=None):
    with socket.socket(socket.AF_UNIX) as conn:
        conn.settimeout(10)
        conn.connect(path)
        data = b"" if body is None else json.dumps(body).encode()
        conn.sendall(f"{method} /api/v1/{endpoint} HTTP/1.1\r\nHost: ch\r\nContent-Length: {len(data)}\r\nConnection: close\r\n\r\n".encode() + data)
        response = http.client.HTTPResponse(conn)
        response.begin()
        value = response.read()
        assert response.status in (200, 204), (response.status, value)
        return json.loads(value) if value else None


call("PUT", "vm.resize", {"desired_balloon": target})
for attempt in range(100):
    info = call("GET", "vm.info")
    actual = info["memory_actual_size"]
    print(json.dumps({"desired_balloon": target, "memory_actual_size": actual}), flush=True)
    if actual == (256 << 20) - target:
        break
    time.sleep(.1)
else:
    raise RuntimeError("balloon did not reach target")
