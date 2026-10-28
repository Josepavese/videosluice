"""RDK codec lifetime contracts using the native SDK state-machine fixture."""
from pathlib import Path
import os
import subprocess
import tempfile
import unittest


class EncoderLifetime(unittest.TestCase):
    def run_contract(self, sanitized):
        root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory(prefix="videosluice-encoder-lifetime-") as work:
            binary = str(Path(work) / "encoder-test")
            flags = ["-fsanitize=address,undefined", "-fno-omit-frame-pointer",
                     "-fno-pie", "-no-pie"] if sanitized else ["-O2"]
            subprocess.run(["cc", "-std=c11", "-Wall", "-Wextra", "-Werror", *flags,
                            "-I" + str(root / "native/tests/stubs"),
                            "-I" + str(root / "native"),
                            str(root / "native/tests/persistent_encoder_test.c"),
                            str(root / "native/rdkx5_persistent_encoder.c"),
                            "-o", binary], check=True, capture_output=True, timeout=60)
            result = subprocess.run([binary], check=True, capture_output=True,
                                    text=True, timeout=60, env=dict(os.environ,
                                    ASAN_OPTIONS="detect_leaks=1:abort_on_error=1",
                                    UBSAN_OPTIONS="halt_on_error=1"))
            self.assertIn("PERSISTENT_ENCODER_RELEASE_PASS", result.stdout)

    def test_sdk_lifetime_and_failed_queue_generation(self):
        self.run_contract(False)

    def test_sdk_lifetime_sanitized(self):
        self.run_contract(True)


if __name__ == "__main__":
    unittest.main()
