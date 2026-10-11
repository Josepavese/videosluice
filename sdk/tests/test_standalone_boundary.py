"""The reusable SDK must not depend on any consuming application."""
import ast
from pathlib import Path
import sys
import unittest


def external_imports(source):
    allowed = sys.stdlib_module_names | {"video_broker_sdk"}
    failures = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            names = [item.name for item in node.names]
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                if node.level > 1:
                    failures.append((node.lineno, "relative import escapes SDK"))
                continue
            names = [node.module or ""]
        else:
            continue
        failures.extend((node.lineno, name) for name in names
                        if name.split(".", 1)[0] not in allowed)
    return failures


class StandaloneBoundary(unittest.TestCase):
    def test_all_sdk_modules_import_only_stdlib_or_the_sdk(self):
        root = Path(__file__).resolve().parents[1] / "python" / "video_broker_sdk"
        paths = sorted(root.rglob("*.py"))
        self.assertTrue(paths, "no SDK source inspected")
        for path in paths:
            with self.subTest(module=str(path.relative_to(root))):
                self.assertEqual(external_imports(path.read_text()), [])

    def test_guard_rejects_consumer_dependencies_even_inside_functions(self):
        source = """
import person_registry.frame_provenance
from providers.detection import BPUEngine
def infer():
    from app.bootstrap import run_tracker
    from ..consumer import state
"""
        self.assertEqual(len(external_imports(source)), 4)

    def test_guard_accepts_generic_transport_and_relative_protocol_imports(self):
        self.assertEqual(external_imports("""
import socket, ctypes
from .causal_envelope import Invalid
from video_broker_sdk.frame_access import Region
"""), [])


if __name__ == "__main__":
    unittest.main()
