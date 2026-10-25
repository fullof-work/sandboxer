#!/usr/bin/env python3
"""Native ABI/entry smoke; the real cgroup race remains in sandbox.cgroup.sh."""
import os
from pathlib import Path
import platform
import struct
import subprocess
import tempfile
import unittest
import uuid

ROOT = Path(__file__).resolve().parents[2]


class InitCgroupAssertionTests(unittest.TestCase):
    def test_exact_root_and_read_failure_are_independent_of_console_output(self):
        source = (ROOT / 'test/e2e/cases/sandbox.cgroup.sh').read_text()
        # The unquoted YAML heredoc removes these dollar escapes before the
        # guest shell sees the command. Execute the shipped command itself.
        command = next(line.strip() for line in source.splitlines()
                       if line.strip().startswith('top_cgroup=')).replace('\\$', '$')
        with tempfile.TemporaryDirectory() as directory:
            cat = Path(directory, 'cat')
            cat.write_text('#!/bin/sh\n'
                           '[ "$1" = /proc/self/cgroup ] || exit 99\n'
                           'printf "CG-TOP-INIT 0::/ console-noise\\n" >&2\n'
                           'printf "%s" "$TEST_CGROUP"\n'
                           'exit "$TEST_READ_STATUS"\n')
            cat.chmod(0o755)
            for value, status, valid in [('0::/\n', 0, True), ('0::/init\n', 0, False),
                                         ('0::/app\n', 0, False), ('', 0, False),
                                         ('0::/\n1::/other\n', 0, False),
                                         ('0::/\n', 7, False)]:
                with self.subTest(value=value, read_status=status):
                    result = subprocess.run(['sh', '-c', command], capture_output=True,
                        env={**os.environ, 'PATH': directory + ':' + os.environ['PATH'],
                             'TEST_CGROUP': value, 'TEST_READ_STATUS': str(status)}, timeout=5)
                    self.assertEqual(result.returncode == 0, valid)


class NativeForkProbeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary = tempfile.TemporaryDirectory(prefix='fork-probe-build-')
        cls.addClassCleanup(cls.temporary.cleanup)
        cls.root = Path(cls.temporary.name)
        cls.binary = cls.root / 'cgroup-fork-probe'
        result = subprocess.run(['make', '-C', str(ROOT), 'e2e-cgroup-fork-probe',
                                 'TARGET_ARCH=' + platform.machine(),
                                 'E2E_FIXTURE_DIR=' + str(cls.root)],
                                text=True, capture_output=True, timeout=30)
        if result.returncode:
            raise AssertionError(result.stdout + result.stderr)

    def test_static_target_elf_has_no_runtime_loader(self):
        data = self.binary.read_bytes()
        self.assertEqual(data[:7], b'\x7fELF\x02\x01\x01')
        self.assertEqual(struct.unpack_from('<H', data, 18)[0],
                         {'x86_64': 62, 'aarch64': 183}[platform.machine()])
        offset = struct.unpack_from('<Q', data, 32)[0]
        size, count = struct.unpack_from('<HH', data, 54)
        kinds = [struct.unpack_from('<I', data, offset + i * size)[0] for i in range(count)]
        self.assertNotIn(2, kinds, 'probe must not depend on PT_DYNAMIC')
        self.assertNotIn(3, kinds, 'probe must not depend on PT_INTERP')

    def invoke(self, program):
        role = 'probe-' + uuid.uuid4().hex
        result_file = Path('/tmp') / ('cg-' + role + '.fast-paths')
        self.addCleanup(lambda: result_file.unlink(missing_ok=True))
        env = dict(os.environ, PROBE_EXPECTED_ROLE=role, PROBE_TEST_ENV='preserved')
        result = subprocess.run([str(self.binary), str(program), role, '/', 'shared', 'false', '0'],
                                env=env, capture_output=True, text=True, timeout=20)
        return result, result_file

    def test_immediate_children_then_exec_preserve_args_and_environment(self):
        program = self.root / ('entry-' + uuid.uuid4().hex + '.sh')
        program.write_text('#!/bin/sh\n'
                           'test "$1" = "$PROBE_EXPECTED_ROLE" || exit 31\n'
                           'test "$PROBE_TEST_ENV" = preserved || exit 32\n'
                           'test "$2:$3:$4:$5" = /:shared:false:0 || exit 33\n'
                           'exit 0\n')
        program.chmod(0o755)
        result, output = self.invoke(program)
        self.assertEqual(result.returncode, 0, result.stderr)
        records = output.read_text().splitlines()
        self.assertEqual(len(records), 128)
        expected = next(line for line in Path('/proc/self/cgroup').read_text().splitlines()
                        if line.startswith('0::'))
        self.assertEqual(set(records), {expected})

    def test_bad_arguments_fail_without_fork(self):
        result = subprocess.run([str(self.binary)], timeout=5)
        self.assertEqual(result.returncode, 125)

    def test_exec_failure_is_reported_after_children_are_reaped(self):
        result, output = self.invoke(self.root / 'missing-program')
        self.assertEqual(result.returncode, 127)
        self.assertEqual(len(output.read_text().splitlines()), 128)


if __name__ == '__main__':
    unittest.main()
