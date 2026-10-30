#!/usr/bin/env python3
"""Bounded, redacted evidence for the snapshot O_DIRECT integrity assertion."""
import json
import os
import re
import stat
import sys

from usage import startup_failure_summary


def read_regular(path, limit, tail=False):
    fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
    with os.fdopen(fd, 'rb') as source:
        info = os.fstat(source.fileno())
        if not stat.S_ISREG(info.st_mode):
            raise ValueError('not a regular file')
        if tail:
            source.seek(max(0, info.st_size - limit))
        elif info.st_size > limit:
            raise ValueError('oversized input')
        return source.read(limit)


def summarize(log, probe, returncode, probe_status):
    summary = startup_failure_summary(log, returncode, 0)
    summary.pop('elapsed_seconds', None)
    summary.pop('deadline_seconds', None)
    summary['probe_status'] = probe_status
    try:
        data = read_regular(log, 65536, tail=True)
        for marker in (b'DIO-ARMED', b'DIO-COMPLETE'):
            summary[marker.decode()] = {
                'occurrences': data.count(marker),
                'exact_lines': len(re.findall(rb'^' + marker + rb'[ \t\r]*$', data, re.M)),
            }
        ticks = re.findall(rb'^TICK ([0-9]{1,12})[ \t\r]*$', data, re.M)
        summary['last_ticks'] = [int(tick) for tick in ticks[-8:]]
        markers = (
            b'Traceback (most recent call last):',
            b'O_DIRECT positive control unsupported:',
            b'cold direct read corrupted',
            b'restored CPU-warmed control lost saved contents',
            b'O_DIRECT restore corrupted untouched bytes:',
            b'app output did not drain within',
            b'app output MUX acknowledgement timed out within',
            b'app output MUX close:',
        )
        summary['workload_markers'] = {marker.decode(): data.count(marker)
                                       for marker in markers if marker in data}
    except (OSError, ValueError):
        summary['log_unavailable'] = True
    try:
        evidence = json.loads(read_regular(probe, 8192))
        result = {}
        if type(evidence.get('trigger_present')) is bool:
            result['trigger_present'] = evidence['trigger_present']
        state = evidence['state']
        if state.get('phase') in ('armed', 'complete'):
            result['phase'] = state['phase']
        for key in ('cold_control_mismatches', 'regions', 'corrupt_regions'):
            value = state.get(key)
            if type(value) is int and 0 <= value <= 131072:
                result[key] = value
        for key in ('mismatches', 'warm_mismatches'):
            value = state.get(key)
            if (type(value) is list and len(value) == 16
                    and all(type(n) is int and 0 <= n <= 4096 for n in value)):
                result[key] = value
        summary['guest_state'] = result
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        summary['guest_state_unavailable'] = True
    return summary


if __name__ == '__main__':
    code = None if sys.argv[3] == 'running' else int(sys.argv[3])
    status = None if sys.argv[4] == 'not_attempted' else int(sys.argv[4])
    print('snapshot restore failure: ' + json.dumps(
        summarize(sys.argv[1], sys.argv[2], code, status), sort_keys=True))
