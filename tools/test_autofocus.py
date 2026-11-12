"""Native focus transaction on a fake ioctl boundary, including ASan/UBSan."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

class Autofocus(unittest.TestCase):
    def test_native_focus_contract(self):
        root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory(prefix='videosluice-autofocus-') as work:
            exe = str(Path(work) / 'focus')
            subprocess.run(['cc', '-std=gnu11', '-Wall', '-Wextra', '-Werror',
                            '-fsanitize=address,undefined', '-fno-omit-frame-pointer',
                            '-fno-pie', '-no-pie', '-I'+str(root/'native'),
                            str(root/'native/tests/v4l2_autofocus_test.c'), '-o', exe],
                           check=True, capture_output=True, timeout=30)
            r = subprocess.run([exe], check=True, capture_output=True, text=True,
                               timeout=10, env=dict(os.environ,
                               ASAN_OPTIONS='detect_leaks=1:abort_on_error=1',
                               UBSAN_OPTIONS='halt_on_error=1'))
            self.assertIn('V4L2_AUTOFOCUS_PASS', r.stdout)
