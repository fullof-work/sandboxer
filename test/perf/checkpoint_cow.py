#!/usr/bin/env python3
"""Repeatable vhost local DIFF benchmark with native filesystem evidence.

Run from the repository in a prepared Go environment, with --tmpdir on the
filesystem under test. Builds once, then runs the same binary at least three
times. Every timed throughput includes the final Drain; frontend and Checkpoint
percentiles are separate. The default 64 MiB write/read working set exceeds the 32 MiB
COW cache. The dedicated hot case intentionally uses only 4 MiB.
Discard uses a fixed 4 MiB range and a 1 MiB cache, with backend GC enabled;
its total commits include concurrent GC. Reclamation stops the background
worker, times GC calls separately from free-list observation, and reads both
the reused current version and the unchanged retained version.
v1 and all v2-xts variants use the same local encryption policy. Results are observations,
not an automatic performance acceptance claim.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import tempfile
import time


def capture(command):
    result = subprocess.run(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    return {"command": command, "status": result.returncode, "output": result.stdout}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tmpdir', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--rounds', type=int, default=3)
    parser.add_argument('--operations', type=int, default=32768)
    parser.add_argument('--working-set-mib', type=int, default=64)
    parser.add_argument('--gc-cycles', type=int, default=3, help='write/checkpoint/delete/GC/reuse cycles for reclamation')
    parser.add_argument('--modes', default='raw|v1|v2-no-version|v2-shared|v2-private|v2-xts-no-version|v2-xts-shared|v2-xts-private')
    parser.add_argument('--workloads', default='sequential|random|partial|hot|mixed|cold-read|frequent-checkpoint')
    args = parser.parse_args()
    if not 64 <= args.working_set_mib <= 4096:
        parser.error('working-set-mib must be between 64 and 4096')
    if args.rounds < 3 or args.operations < args.working_set_mib * 512:
        parser.error('use at least three rounds and two working-set traversals (working-set-mib * 512 operations)')
    if args.gc_cycles < 3:
        parser.error('gc-cycles must be at least three to observe reuse')
    args.tmpdir = args.tmpdir.resolve()
    args.output = args.output.resolve()
    args.tmpdir.mkdir(parents=True, exist_ok=True)
    args.output.mkdir(parents=True, exist_ok=True)
    repo = Path(__file__).resolve().parents[2]
    os.chdir(repo)
    env = os.environ.copy()
    env['TMPDIR'] = str(args.tmpdir)
    env['CHECKPOINT_BENCH_MIB'] = str(args.working_set_mib)
    modes = args.modes.split('|')
    workloads = args.workloads.split('|')
    known_modes = {'raw', 'v1', 'v2-no-version', 'v2-shared', 'v2-private', 'v2-xts-no-version', 'v2-xts-shared', 'v2-xts-private'}
    known_workloads = {'first-sequential', 'first-random', 'sequential', 'random', 'partial', 'hot', 'mixed', 'cold-read', 'frequent-checkpoint', 'gc-write', 'reclamation', 'background-gc', 'discard'}
    if len(set(modes)) != len(modes) or set(modes) - known_modes:
        parser.error('modes must be unique exact names separated by |')
    if len(set(workloads)) != len(workloads) or set(workloads) - known_workloads:
        parser.error('workloads must be unique exact names separated by |')
    # The official workbench mounts sources but not the worktree git directory.
    # Hash exact Go sources as reproducible revision evidence even in that case.
    sources = sorted(list((repo / 'pkg' / 'diffstore').rglob('*.go')) + [repo / 'go.mod', repo / 'go.sum'])
    metadata = {
        'source_sha256': {str(path.relative_to(repo)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sources},
        'order': 'round 1 supplied order, round 2 reverse, round 3 rotated; each workload interleaves all selected modes',
        'started_unix': time.time(), 'host': platform.uname()._asdict(),
        'working_set_bytes': args.working_set_mib << 20, 'cow_cache_bytes': 32 << 20,
        'operations': args.operations, 'rounds': args.rounds,
        'gc_cycles': args.gc_cycles,
        'workload_parameters': {
            'background-gc': {'backlog_payload_bytes': 4 << 20, 'cycles_per_round': 1,
                              'time': 'worker startup through Complete, including scheduling gaps'},
            'discard': {'range_bytes': 4 << 20, 'cow_cache_bytes': 1 << 20,
                        'semantics': 'Zero: retain logical presence and remove active payload',
                        'background_gc': True, 'commits': 'total, including concurrent GC'},
            'reclamation': {'working_set_bytes': args.working_set_mib << 20,
                            'background_gc': False, 'gc_time': 'sum of GC calls, excluding free-list observation',
                            'backlog_wall_time': 'includes committed free-list observation'},
        },
        'tmpdir': str(args.tmpdir), 'modes': args.modes, 'workloads': args.workloads,
        'go_environment': {name: env.get(name) for name in ('GOMAXPROCS', 'GOMEMLIMIT', 'GOGC', 'GOAMD64', 'GOOS', 'GOARCH')},
        'cgroups': {name: (Path('/sys/fs/cgroup') / name).read_text() for name in ('cpu.max', 'memory.max') if (Path('/sys/fs/cgroup') / name).is_file()},
        'git': capture(['git', 'rev-parse', 'HEAD']),
        'dirty': capture(['git', 'status', '--short']),
        'go': capture(['go', 'version']),
        'cpu': capture(['lscpu']),
        'filesystem': capture(['findmnt', '-T', str(args.tmpdir)]),
        'capacity': capture(['df', '-hT', str(args.tmpdir)]),
    }
    (args.output / 'environment.json').write_text(json.dumps(metadata, indent=2) + '\n')
    with tempfile.TemporaryDirectory(prefix='checkpoint-perf-', dir=args.tmpdir) as build:
        binary = str(Path(build) / 'vhost.test')
        subprocess.run(['go', 'test', '-c', '-o', binary, './pkg/diffstore'], env=env, check=True)
        gc_binary = str(Path(build) / 'versioned.test')
        if set(workloads) & {'reclamation', 'background-gc'}:
            subprocess.run(['go', 'test', '-c', '-o', gc_binary, './pkg/diffstore/internal/versioned'], env=env, check=True)
        metrics = []
        for iteration in range(1, args.rounds + 1):
            order = list(reversed(modes)) if iteration % 2 == 0 else modes[(iteration - 1) // 2:] + modes[:(iteration - 1) // 2]
            for workload in workloads:
                for mode in order:
                    if workload.startswith('first-') and mode.endswith('-private'):
                        continue
                    if workload in ('frequent-checkpoint', 'gc-write', 'reclamation', 'background-gc') and mode in ('raw', 'v1'):
                        continue
                    if workload in ('discard', 'reclamation', 'background-gc') and mode not in ('raw', 'v1', 'v2-no-version', 'v2-xts-no-version'):
                        continue
                    name, executable, operations = 'BenchmarkCheckpointCOWWriteback', binary, args.operations
                    if workload.startswith('first-'):
                        operations = args.working_set_mib * 256  # one first write per page, no overwrite
                    bench = '^' + name + '$/^' + re.escape(mode) + '$/^' + re.escape(workload) + '$'
                    if workload == 'reclamation':
                        name, executable, operations = 'BenchmarkCheckpointReclamation', gc_binary, args.gc_cycles
                        bench = '^' + name + '$/^' + ('xts' if '-xts-' in mode else 'plain') + '$'
                    elif workload == 'background-gc':
                        name, executable, operations = 'BenchmarkCheckpointBackgroundGC', gc_binary, 1
                        bench = '^' + name + '$/^' + ('xts' if '-xts-' in mode else 'plain') + '$'
                    elif workload == 'discard':
                        name, operations = 'BenchmarkCheckpointDiscard', args.gc_cycles
                        bench = '^' + name + '$/^' + re.escape(mode) + '$'
                    command = [executable, '-test.run=^$', '-test.bench=' + bench,
                               '-test.benchtime=' + str(operations) + 'x', '-test.timeout=60m']
                    stem = 'round-%02d-%s-%s' % (iteration, workload, mode)
                    log = args.output / (stem + '.log')
                    start = time.monotonic()
                    with log.open('w') as out:
                        process = subprocess.Popen(command, env=env, stdout=out, stderr=subprocess.STDOUT)
                        _, status, after = os.wait4(process.pid, 0)
                        process.returncode = os.waitstatus_to_exitcode(status)
                    usage = {'round': iteration, 'mode': mode, 'workload': workload,
                             'command': command, 'status': process.returncode,
                             'wall_seconds': time.monotonic() - start,
                             'user_seconds': after.ru_utime,
                             'system_seconds': after.ru_stime,
                             'peak_child_rss_kib': after.ru_maxrss,
                             'input_blocks': after.ru_inblock,
                             'output_blocks': after.ru_oublock,
                             'note': 'CPU and process peak RSS include fixture initialization; benchmark ns/op excludes it.'}
                    (args.output / (stem + '.resources.json')).write_text(json.dumps(usage, indent=2) + '\n')
                    print(log.read_text(), flush=True)
                    if process.returncode:
                        raise SystemExit('benchmark failed; raw log: ' + str(log))
                    found = 0
                    for line in log.read_text().splitlines():
                        if not line.startswith(name + '/'):
                            continue
                        fields = line.split()
                        if len(fields) < 4 or not fields[1].isdigit():
                            continue
                        row = {'round': iteration, 'mode': mode, 'workload': workload, 'case': re.sub(r'-\d+$', '', fields[0]), 'operations': int(fields[1])}
                        for i in range(2, len(fields) - 1, 2):
                            row[fields[i + 1]] = float(fields[i])
                        metrics.append(row)
                        found += 1
                    if found != 1:
                        raise SystemExit('expected one exact benchmark result; raw log: ' + str(log))
                    (args.output / 'metrics.json').write_text(json.dumps(metrics, indent=2) + '\n')
    print('Evidence saved to', args.output)


if __name__ == '__main__':
    main()
