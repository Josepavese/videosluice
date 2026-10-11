"""Native executable contract vectors, structural mutations and sanitizer run."""
import os
import subprocess
import unittest
from crosscheck import vectors, accepted


class Contract(unittest.TestCase):
    def test_independent_native_vectors_and_mutations(self):
        executable = os.environ["VBCF_TEST_ORACLE"]
        produced = subprocess.run([executable, "--vectors"], check=True,
                                  capture_output=True, text=True, timeout=15)
        expected = vectors()
        self.assertEqual(produced.stdout.splitlines(), [v.hex() for v in expected])
        inputs = list(expected)
        for packet in expected:
            for offset in range(len(packet)):
                invalid = bytearray(packet)
                invalid[offset] ^= 128
                inputs.append(bytes(invalid))
        for packet in expected:
            inputs.extend(packet[:n] for n in range(len(packet)))
            inputs.append(packet+b"\0")
        wire = "".join(v.hex()+"\n" for v in inputs)
        result = subprocess.run([executable], input=wire, capture_output=True,
                                text=True, timeout=30, check=True)
        self.assertEqual(result.stderr, "")
        wanted = ["1" if accepted(v.hex()) else "0" for v in inputs]
        self.assertEqual(result.stdout.splitlines(), wanted)
        print(f"NATIVE_PARITY_PASS cases={len(inputs)} independent_vectors={len(expected)}")


if __name__ == "__main__":
    unittest.main()
