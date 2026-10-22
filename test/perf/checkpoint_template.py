#!/usr/bin/env python3
"""Measure actual 64 MiB template materialization: three rounds, raw/v1/v2/XTS."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tmpdir', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.tmpdir = args.tmpdir.resolve()
    args.output = args.output.resolve()
    args.tmpdir.mkdir(parents=True, exist_ok=True)
    args.output.mkdir(parents=True, exist_ok=True)
    repo = Path(__file__).resolve().parents[2]
    os.chdir(repo)
    env = os.environ.copy()
    env['TMPDIR'] = str(args.tmpdir)
    sources = sorted(list((repo / 'pkg/vhost').glob('*.go')) + list((repo / 'internal/diffstore').glob('*.go'))) + [repo / 'go.mod', repo / 'go.sum']
    metadata = {'started_unix': time.time(), 'template_payload_bytes': 64 << 20, 'rounds': 3,
                'source_sha256': {str(path.relative_to(repo)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sources},
                'go_version': subprocess.check_output(['go', 'version'], text=True),
                'filesystem': subprocess.check_output(['findmnt', '-T', str(args.tmpdir)], text=True),
                'note': 'Shared/private version states do not exist before fresh initialization, so four distinct physical initialization paths are measured.'}
    (args.output / 'environment.json').write_text(json.dumps(metadata, indent=2) + '\n')
    rows = []
    modes = ['raw', 'v1', 'v2', 'v2-xts']
    with tempfile.TemporaryDirectory(prefix='template-perf-', dir=args.tmpdir) as temporary:
        binary = str(Path(temporary) / 'vhost.test')
        subprocess.run(['go', 'test', '-c', '-o', binary, './pkg/vhost'], env=env, check=True)
        for round in range(1, 4):
            order = modes if round == 1 else list(reversed(modes)) if round == 2 else modes[1:] + modes[:1]
            for mode in order:
                env['VHOST_TEMPLATE_PERF_MODE'] = mode
                env['VHOST_TEMPLATE_PERF_ROUND'] = str(round)
                command = [binary, '-test.v', '-test.run=^TestCheckpointTemplatePhases$', '-test.timeout=5m']
                log = args.output / f'round-{round:02d}-{mode}.log'
                with log.open('w') as output:
                    result = subprocess.run(command, env=env, stdout=output, stderr=subprocess.STDOUT)
                if result.returncode:
                    raise SystemExit(f'template measurement failed; see {log}')
                samples = [json.loads(line[len('vhost-template '):]) for line in log.read_text().splitlines() if line.startswith('vhost-template ')]
                if len(samples) != 1 or samples[0]['mode'] != mode or samples[0]['round'] != round:
                    raise SystemExit(f'expected one exact template result; see {log}')
                rows.extend(samples)
                (args.output / 'metrics.json').write_text(json.dumps(rows, indent=2) + '\n')
                print(json.dumps(samples[0]), flush=True)
    print('Saved 12 verified template samples to', args.output, flush=True)


if __name__ == '__main__':
    main()
