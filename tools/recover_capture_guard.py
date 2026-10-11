#!/usr/bin/env python3
"""Offline, audited recovery after an operator-confirmed hardware power cycle.

The embedding application must stop all supplied capture owners under its own
maintenance lock. This tool neither restarts services nor resets hardware.
It preserves the exact incident and a reset attestation outside the active
guard, rather than silently deleting a safety latch on daemon/OS restart.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import uuid
from datetime import datetime, timezone


class RecoveryError(RuntimeError):
    pass


def parse_created_at(value):
    # Go writes RFC3339Nano; Python 3.10 accepts microseconds, not nine digits.
    match = re.fullmatch(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})", value)
    if not match:
        raise RecoveryError("guard creation time is not RFC3339Nano")
    whole, fraction, zone = match.groups()
    if fraction:
        whole += "." + fraction[:6].ljust(6, "0")
    return datetime.fromisoformat(whole + ("+00:00" if zone == "Z" else zone))


def boot_id(value):
    try:
        result = str(uuid.UUID(value))
    except (ValueError, AttributeError) as exc:
        raise RecoveryError("invalid boot identity") from exc
    if result != value:
        raise RecoveryError("boot identity must be canonical")
    return result


def secure_directory(path, allowed_owners=(0,)):
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid not in allowed_owners or info.st_mode & 0o022:
        raise RecoveryError("state directory is not protected or has an unexpected owner")


def read_document(path, allowed_owners=(0,)):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid not in allowed_owners or info.st_mode & 0o022 or info.st_size > 16384:
            raise RecoveryError("incident document has unsafe ownership/type/size")
        body = os.read(fd, 16385)
        if len(body) > 16384:
            raise RecoveryError("incident document too large")
        return body, info
    finally:
        os.close(fd)


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def assert_owners_stopped(executables, proc=Path("/proc")):
    if not executables:
        raise RecoveryError("capture owner executable list is required")
    owners = {str(path.resolve(strict=True)) for path in executables}
    for entry in proc.iterdir():
        if not entry.name.isdecimal():
            continue
        try:
            executable = os.readlink(entry / "exe")
        except FileNotFoundError:
            continue
        except PermissionError as exc:
            raise RecoveryError("cannot verify process quiescence") from exc
        if executable.removesuffix(" (deleted)") in owners:
            raise RecoveryError(f"capture owner is still running pid={entry.name}")


def recover(guard_path, incident_id, expected_sha256, previous_boot, current_boot,
            executables, *, recovery_root, confirmed_power_cycle=False, state_owner_uid=0, proc=Path("/proc")):
    if os.geteuid() != 0:
        raise RecoveryError("recovery requires the administrative owner")
    if state_owner_uid < 0:
        raise RecoveryError("invalid state owner UID")
    owners = (0, state_owner_uid)
    previous_boot, current_boot = boot_id(previous_boot), boot_id(current_boot)
    if not confirmed_power_cycle or previous_boot == current_boot:
        raise RecoveryError("a confirmed hardware power cycle into a different boot is required")
    if not guard_path.is_absolute() or guard_path.name != "frame-cleanup-guard.json":
        raise RecoveryError("exact absolute guard path is required")
    # Refuse symlink ancestors, not only the final component.
    for parent in reversed(guard_path.parents):
        secure_directory(parent, owners)
    # Runtime state can be reassigned to the capture account during upgrades.
    # Reset attestations belong to an explicitly selected administrative PAL,
    # not a child of that service-owned tree. Never relax archive ownership.
    if not recovery_root.is_absolute() or recovery_root == Path("/") or \
            recovery_root == guard_path.parent or guard_path.parent in recovery_root.parents or \
            recovery_root in guard_path.parents:
        raise RecoveryError("an independent absolute administrative recovery root is required")
    for parent in reversed(recovery_root.parents):
        secure_directory(parent)
    if len(expected_sha256) != 64 or any(c not in "0123456789abcdef" for c in expected_sha256):
        raise RecoveryError("exact guard SHA-256 is required")
    if not incident_id or len(incident_id) > 128 or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for c in incident_id):
        raise RecoveryError("invalid incident identity")
    assert_owners_stopped(executables, proc)
    destination = recovery_root / (incident_id + "-" + expected_sha256)
    if not guard_path.exists():
        secure_directory(recovery_root)
        secure_directory(destination)
        archived, _ = read_document(destination / guard_path.name, owners)
        receipt, _ = read_document(destination / "reset-receipt.json")
        record = json.loads(receipt)
        if hashlib.sha256(archived).hexdigest() != expected_sha256 or record.get("reset_boot_id") != current_boot or record.get("prior_boot_id") != previous_boot or record.get("incident_id") != incident_id:
            raise RecoveryError("archived recovery does not match this request")
        return {"recovered": True, "already_recovered": True, "archive": str(destination)}
    body, identity = read_document(guard_path, owners)
    if hashlib.sha256(body).hexdigest() != expected_sha256:
        raise RecoveryError("guard changed; recovery request is stale")
    guard = json.loads(body)
    if guard.get("schema") != "video-broker.frame-cleanup-guard.v1" or guard.get("incident_id") != incident_id or guard.get("reason") not in {"provider_release_unconfirmed", "cleanup_pending"}:
        raise RecoveryError("guard identity/schema/reason is invalid")
    if guard.get("boot_id") and guard["boot_id"] != previous_boot:
        raise RecoveryError("guard belongs to another boot")
    created = parse_created_at(guard["created_at"])
    if created.tzinfo is None or created > datetime.now(timezone.utc):
        raise RecoveryError("guard creation time is invalid")
    recovery_root.mkdir(mode=0o700, exist_ok=True)
    secure_directory(recovery_root)
    destination.mkdir(mode=0o700, exist_ok=True)
    secure_directory(destination)
    if (destination / guard_path.name).exists():
        raise RecoveryError("archive already contains a guard; refusing overwrite")
    receipt = {"schema": "video-broker.hardware-reset-receipt.v1",
               "incident_id": incident_id, "guard_sha256": expected_sha256,
               "prior_boot_id": previous_boot, "reset_boot_id": current_boot,
               "reset_method": "operator_confirmed_hardware_power_cycle",
               "legacy_guard_without_boot_id": not bool(guard.get("boot_id")),
               "recorded_at": datetime.now(timezone.utc).isoformat()}
    receipt_path = destination / "reset-receipt.json"
    if receipt_path.exists():
        prior, _ = read_document(receipt_path)
        prior = json.loads(prior)
        if any(prior.get(key) != value for key, value in receipt.items() if key != "recorded_at"):
            raise RecoveryError("existing reset receipt does not match this request")
    else:
        fd = os.open(receipt_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        try:
            payload = json.dumps(receipt, sort_keys=True).encode() + b"\n"
            with os.fdopen(fd, "wb", closefd=False) as output:
                output.write(payload)
                output.flush()
            os.fsync(fd)
        finally:
            os.close(fd)
    sync_directory(destination)
    sync_directory(recovery_root)
    assert_owners_stopped(executables, proc)
    current = guard_path.lstat()
    if (current.st_dev, current.st_ino, current.st_size, current.st_mtime_ns) != (identity.st_dev, identity.st_ino, identity.st_size, identity.st_mtime_ns):
        raise RecoveryError("guard changed during recovery; admission remains held")
    os.rename(guard_path, destination / guard_path.name)
    sync_directory(destination)
    sync_directory(guard_path.parent)
    return {"recovered": True, "already_recovered": False, "archive": str(destination)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--guard", type=Path, required=True)
    parser.add_argument("--incident-id", required=True)
    parser.add_argument("--expected-guard-sha256", required=True)
    parser.add_argument("--recovery-root", type=Path, required=True,
                        help="root-owned administrative archive, outside the capture state tree")
    parser.add_argument("--previous-boot-id", required=True)
    parser.add_argument("--confirm-hardware-power-cycle", action="store_true")
    parser.add_argument("--owner-executable", type=Path, action="append", required=True)
    parser.add_argument("--state-owner-uid", type=int, required=True)
    args = parser.parse_args()
    try:
        result = recover(args.guard, args.incident_id, args.expected_guard_sha256,
                         args.previous_boot_id, Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
                         args.owner_executable, confirmed_power_cycle=args.confirm_hardware_power_cycle,
                         recovery_root=args.recovery_root, state_owner_uid=args.state_owner_uid)
    except (RecoveryError, OSError, ValueError, KeyError) as exc:
        parser.exit(1, f"capture recovery rejected: {exc}\n")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
