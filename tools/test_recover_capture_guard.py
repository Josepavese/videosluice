import hashlib
import importlib.util
import json
import os
from pathlib import Path
import stat
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("recovery", Path(__file__).with_name("recover_capture_guard.py"))
recovery = importlib.util.module_from_spec(spec)
spec.loader.exec_module(recovery)
real_secure_directory = recovery.secure_directory
OLD = "891288b0-229e-443f-9960-fe505bbbc4a5"
NEW = "ce10d046-ca26-4f56-b591-b7b8c860695b"


class CaptureRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        camera = self.root / "camera"
        camera.mkdir()
        self.guard = camera / "frame-cleanup-guard.json"
        self.archive_root = self.root / "admin-recoveries"
        self.body = json.dumps({"schema": "video-broker.frame-cleanup-guard.v1",
                               "incident_id": "incident-7", "reason": "provider_release_unconfirmed",
                               "created_at": "2026-09-24T15:32:25.905907016Z", "boot_id": OLD}).encode()
        self.guard.write_bytes(self.body)
        self.sha = hashlib.sha256(self.body).hexdigest()
        self.owner = self.root / "native-worker"
        self.owner.touch()
        self.proc = self.root / "proc"
        self.proc.mkdir()
        # Unit fixtures are user-owned; production always requires UID 0.
        self.addCleanup(patch.stopall)
        patch.object(recovery.os, "geteuid", return_value=0).start()
        patch.object(recovery, "secure_directory", side_effect=lambda p, *a: self.assertTrue(p.is_dir() and not p.is_symlink())).start()
        patch.object(recovery, "read_document", side_effect=lambda p, *a: (p.read_bytes(), p.stat())).start()

    def run_recovery(self, **overrides):
        args = dict(guard_path=self.guard, incident_id="incident-7", expected_sha256=self.sha,
                    previous_boot=OLD, current_boot=NEW, executables=[self.owner],
                    recovery_root=self.archive_root, confirmed_power_cycle=True, proc=self.proc)
        args.update(overrides)
        return recovery.recover(**args)

    def test_preserves_incident_and_receipt_and_is_idempotent(self):
        result = self.run_recovery()
        archive = Path(result["archive"])
        self.assertFalse(self.guard.exists())
        self.assertEqual((archive / self.guard.name).read_bytes(), self.body)
        receipt = json.loads((archive / "reset-receipt.json").read_bytes())
        self.assertEqual(receipt["reset_boot_id"], NEW)
        self.assertEqual(receipt["guard_sha256"], self.sha)
        self.assertEqual(stat.S_IMODE((archive / "reset-receipt.json").stat().st_mode), 0o600)
        self.assertTrue(self.run_recovery()["already_recovered"])

    def test_same_boot_missing_confirmation_stale_guard_and_wrong_incident_rejected(self):
        for args in ({"current_boot": OLD}, {"confirmed_power_cycle": False},
                     {"expected_sha256": "0" * 64}, {"incident_id": "other"},
                     {"previous_boot": "00000000-0000-0000-0000-000000000000"}):
            with self.subTest(args=args), self.assertRaises(recovery.RecoveryError):
                self.run_recovery(**args)
            self.assertTrue(self.guard.exists())

    def test_running_owner_rejected(self):
        process = self.proc / "123"
        process.mkdir()
        (process / "exe").symlink_to(self.owner)
        with self.assertRaises(recovery.RecoveryError):
            self.run_recovery()
        self.assertTrue(self.guard.exists())

    def test_archive_is_outside_camera_state_and_old_archives_are_preserved(self):
        historical = self.guard.parent / "frame-cleanup-recoveries"
        historical.mkdir()
        retained = historical / "old-incident.txt"
        retained.write_bytes(b"retained evidence")
        result = self.run_recovery()
        self.assertEqual(Path(result['archive']).parent, self.archive_root)
        self.assertEqual(retained.read_bytes(), b"retained evidence")

    def test_relative_or_capture_owned_archive_namespace_rejected(self):
        for path in (Path("relative"), Path("/"), self.guard.parent,
                     self.guard.parent / "frame-cleanup-recoveries", self.guard.parent.parent):
            with self.subTest(path=path), self.assertRaises(recovery.RecoveryError):
                self.run_recovery(recovery_root=path)
            self.assertTrue(self.guard.exists())

    def test_administrative_archive_never_accepts_service_owner(self):
        original_check = recovery.secure_directory
        def verify(path, allowed_owners=(0,)):
            if path == self.archive_root:
                self.assertEqual(allowed_owners, (0,))
                raise recovery.RecoveryError('archive is owned by the runtime service')
            original_check(path, allowed_owners)
        with patch.object(recovery, 'secure_directory', side_effect=verify):
            with self.assertRaises(recovery.RecoveryError):
                self.run_recovery()
        self.assertTrue(self.guard.exists())

    def test_real_directory_validator_rejects_runtime_owner_writes_and_symlink(self):
        for mode, uid in ((stat.S_IFDIR | 0o700, 999),
                          (stat.S_IFDIR | 0o770, 0),
                          (stat.S_IFLNK | 0o777, 0)):
            fake = SimpleNamespace(lstat=lambda: SimpleNamespace(st_mode=mode, st_uid=uid))
            with self.subTest(mode=mode, uid=uid), self.assertRaises(recovery.RecoveryError):
                real_secure_directory(fake)
        real_secure_directory(SimpleNamespace(lstat=lambda: SimpleNamespace(st_mode=stat.S_IFDIR | 0o700, st_uid=0)))

    def test_go_nanosecond_timestamp_and_invalid_date(self):
        self.assertEqual(recovery.parse_created_at("2026-09-24T15:32:25.905907016Z").microsecond, 905907)
        for value in ("2026-09-24", "2026-09-24T15:32:25.1234567890Z", "2026-13-24T15:32:25Z"):
            with self.subTest(value=value), self.assertRaises((ValueError, recovery.RecoveryError)):
                recovery.parse_created_at(value)

    def test_crash_before_archive_retries_same_receipt(self):
        with patch.object(recovery.os, "rename", side_effect=OSError("simulated crash")):
            with self.assertRaises(OSError):
                self.run_recovery()
        self.assertTrue(self.guard.exists())
        self.run_recovery()
        self.assertFalse(self.guard.exists())

    def test_changed_incident_not_archived(self):
        check = recovery.assert_owners_stopped
        calls = 0
        def replace_incident(*args):
            nonlocal calls
            calls += 1
            check(*args)
            if calls == 2:
                self.guard.write_bytes(b"replacement")
        with patch.object(recovery, "assert_owners_stopped", side_effect=replace_incident):
            # The final identity check must occur AFTER the second quiescence
            # check, immediately before the archive rename.
            with self.assertRaises(recovery.RecoveryError):
                self.run_recovery()
        self.assertEqual(self.guard.read_bytes(), b"replacement")


if __name__ == "__main__":
    unittest.main()
