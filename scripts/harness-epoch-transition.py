#!/usr/bin/env python3
"""Crash-recoverable activation of one Harness database/changeset epoch pair.

The active database and active semantic-log directory are switched under the
same exclusive lock observed by harness-cli mutation commands. A checksummed
journal is rewritten and fsynced after every rename. `recover forward` finishes
the named pair; `recover compensate` restores the exact legacy pair.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import shutil
import sys
from pathlib import Path
from typing import Any

FORMAT_VERSION = 1
RENAME_STEPS = (
    "legacy_db_archived",
    "legacy_log_archived",
    "fresh_db_activated",
    "fresh_log_activated",
)


class TransitionError(RuntimeError):
    pass


def canonical(value: Any) -> bytes:
    # Rust serde_json::to_vec emits compact JSON while preserving map order.
    # Payload objects are constructed in sorted-key order and read with Python's
    # insertion-order-preserving decoder, so both implementations hash the same bytes.
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def tree_sha256(path: Path) -> str:
    if not path.is_dir():
        raise TransitionError(f"changeset directory is missing: {path}")
    records = []
    for child in sorted(path.rglob("*")):
        relative = child.relative_to(path).as_posix()
        if child.is_symlink() or not child.is_file():
            if child.is_dir() and not child.is_symlink():
                continue
            raise TransitionError(f"unsupported changeset entry: {child}")
        records.append({"path": relative, "sha256": file_sha256(child), "size": child.stat().st_size})
    return sha256_bytes(canonical(records))


def fsync_dir(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def write_journal(path: Path, payload: dict[str, Any]) -> None:
    payload = dict(sorted(payload.items()))
    envelope = {
        "payload": payload,
        "payload_sha256": sha256_bytes(canonical(payload)),
    }
    temporary = path.with_suffix(".tmp")
    with temporary.open("wb") as output:
        output.write(canonical(envelope) + b"\n")
        output.flush()
        os.fsync(output.fileno())
    os.replace(temporary, path)
    fsync_dir(path.parent)


def read_journal(path: Path) -> dict[str, Any]:
    try:
        envelope = json.loads(path.read_text())
        payload = envelope["payload"]
        declared = envelope["payload_sha256"]
    except (OSError, KeyError, json.JSONDecodeError) as error:
        raise TransitionError(f"invalid transition journal {path}: {error}") from error
    actual = sha256_bytes(canonical(payload))
    if declared != actual:
        raise TransitionError(
            f"transition journal checksum mismatch: declared {declared}, calculated {actual}"
        )
    if payload.get("format_version") != FORMAT_VERSION:
        raise TransitionError("unsupported transition journal format")
    return payload


def require_file(path: Path, label: str) -> None:
    if not path.is_file() or path.is_symlink():
        raise TransitionError(f"{label} must be a regular file: {path}")


def require_same_device(paths: list[Path]) -> None:
    devices = {path.parent.stat().st_dev for path in paths}
    if len(devices) != 1:
        raise TransitionError("all transition paths must have parents on the same filesystem")


def paths(payload: dict[str, Any]) -> dict[str, Path]:
    return {key: Path(value) for key, value in payload["paths"].items()}


def verify_pair(payload: dict[str, Any], generation: str) -> None:
    item = paths(payload)
    if generation == "fresh":
        db, log = item["active_db"], item["active_log"]
        hashes = payload["fresh"]
    else:
        db, log = item["active_db"], item["active_log"]
        hashes = payload["legacy"]
    require_file(db, f"{generation} active database")
    if file_sha256(db) != hashes["db_sha256"]:
        raise TransitionError(f"{generation} active database checksum mismatch")
    if tree_sha256(log) != hashes["log_sha256"]:
        raise TransitionError(f"{generation} active changeset checksum mismatch")


def rename_and_record(
    payload: dict[str, Any], journal: Path, source: Path, destination: Path, step: str, inject: str | None
) -> None:
    _checked_replace(source, destination, "rename destination already exists")
    # Injection is deliberately before the journal update: this is the hardest
    # real crash boundary. Recovery must infer the completed rename from hashes.
    if inject == step:
        raise TransitionError(f"injected crash after {step}")
    payload["completed_steps"].append(step)
    payload["state"] = step
    write_journal(journal, payload)


def _checked_replace(source: Path, destination: Path, error_label: str) -> None:
    """Atomic rename with destination-exists guard and parent-dir fsync.

    Shared between the forward path (`rename_and_record`) and the compensation
    path (`compensate`) so both keep the same I/O shape and the same failure
    mode when the destination already exists. The journal update and step
    bookkeeping remain at the call site because the two paths label `state`
    differently and the forward path also tracks `completed_steps`.
    """
    if destination.exists():
        raise TransitionError(f"{error_label}: {destination}")
    os.replace(source, destination)
    fsync_dir(destination.parent)


def reconcile_unjournaled_renames(payload: dict[str, Any], journal: Path) -> None:
    item = paths(payload)
    completed = set(payload["completed_steps"])
    expected = {
        "legacy_db_archived": (item["legacy_db"], payload["legacy"]["db_sha256"], False),
        "legacy_log_archived": (item["legacy_log"], payload["legacy"]["log_sha256"], True),
        "fresh_db_activated": (item["active_db"], payload["fresh"]["db_sha256"], False),
        "fresh_log_activated": (item["active_log"], payload["fresh"]["log_sha256"], True),
    }
    changed = False
    for step in RENAME_STEPS:
        if step in completed:
            continue
        destination, digest, is_tree = expected[step]
        if not destination.exists():
            continue
        actual = tree_sha256(destination) if is_tree else file_sha256(destination)
        if actual != digest:
            # An active legacy path may legitimately exist before its archive
            # step; only a matching destination proves an unjournaled rename.
            continue
        payload["completed_steps"].append(step)
        payload["state"] = step
        completed.add(step)
        changed = True
    if changed:
        write_journal(journal, payload)


def finish_forward(payload: dict[str, Any], journal: Path, inject: str | None = None) -> None:
    reconcile_unjournaled_renames(payload, journal)
    item = paths(payload)
    operations = (
        (item["active_db"], item["legacy_db"], "legacy_db_archived"),
        (item["active_log"], item["legacy_log"], "legacy_log_archived"),
        (item["fresh_db"], item["active_db"], "fresh_db_activated"),
        (item["fresh_log"], item["active_log"], "fresh_log_activated"),
    )
    completed = set(payload["completed_steps"])
    for source, destination, step in operations:
        if step in completed:
            if not destination.exists():
                raise TransitionError(f"journal records {step}, but destination is missing")
            continue
        rename_and_record(payload, journal, source, destination, step, inject)
        completed.add(step)
    verify_pair(payload, "fresh")
    payload["state"] = "switched_pending_validation"
    write_journal(journal, payload)


def compensate(payload: dict[str, Any], journal: Path) -> None:
    reconcile_unjournaled_renames(payload, journal)
    item = paths(payload)
    completed = set(payload["completed_steps"])
    # Compensation reverses the forward order: move fresh paths back into
    # place, then restore the legacy pair. Each step must be checkpointed
    # in the journal before the next one runs so a crash mid-compensation
    # leaves the system resumable instead of half-recovered. The previous
    # implementation did all four `os.replace` calls in a row, so a single
    # mid-flight crash left the tool unable to recover itself.
    compensations = (
        ("fresh_log_activated", item["active_log"], item["fresh_log"]),
        ("fresh_db_activated", item["active_db"], item["fresh_db"]),
        ("legacy_log_archived", item["legacy_log"], item["active_log"]),
        ("legacy_db_archived", item["legacy_db"], item["active_db"]),
    )
    for step, source, destination in compensations:
        if step not in completed:
            continue
        _checked_replace(source, destination, "compensation destination already exists")
        payload["state"] = f"compensating:{step}"
        write_journal(journal, payload)
    verify_pair(payload, "legacy")
    payload["state"] = "compensated"
    payload["completed_steps"] = []
    write_journal(journal, payload)


def begin(args: argparse.Namespace, journal: Path) -> None:
    active_db = args.repo_root / args.active_db
    active_log = args.repo_root / args.active_log
    fresh_db = args.fresh_db.resolve()
    fresh_log = args.fresh_log.resolve()
    archive = args.archive_root.resolve()
    archive.mkdir(parents=True, exist_ok=True)
    legacy_db = archive / "legacy-harness.db"
    legacy_log = archive / "legacy-changesets"
    require_file(active_db, "active database")
    require_file(fresh_db, "fresh database")
    legacy_log_hash = tree_sha256(active_log)
    fresh_log_hash = tree_sha256(fresh_log)
    for destination in (legacy_db, legacy_log):
        if destination.exists():
            raise TransitionError(f"archive destination already exists: {destination}")
    require_same_device([active_db, active_log, fresh_db, fresh_log, legacy_db, legacy_log])
    payload = {
        "completed_steps": [],
        "format_version": FORMAT_VERSION,
        "fresh": {"db_sha256": file_sha256(fresh_db), "log_sha256": fresh_log_hash},
        "legacy": {"db_sha256": file_sha256(active_db), "log_sha256": legacy_log_hash},
        "paths": {
            "active_db": str(active_db.resolve()),
            "active_log": str(active_log.resolve()),
            "fresh_db": str(fresh_db),
            "fresh_log": str(fresh_log),
            "legacy_db": str(legacy_db),
            "legacy_log": str(legacy_log),
        },
        "state": "prepared",
        "transition_id": args.transition_id,
    }
    write_journal(journal, payload)
    if args.inject_after == "prepared":
        raise TransitionError("injected crash after prepared")
    finish_forward(payload, journal, args.inject_after)


def fence(journal: Path, transition_id: str) -> None:
    payload = {
        "format_version": FORMAT_VERSION,
        "state": "fenced",
        "transition_id": transition_id,
    }
    write_journal(journal, payload)


def complete(journal: Path, transition_id: str) -> None:
    payload = read_journal(journal)
    if payload["transition_id"] != transition_id:
        raise TransitionError("transition id does not match journal")
    if payload["state"] != "switched_pending_validation":
        raise TransitionError(f"transition cannot complete from state {payload['state']}")
    verify_pair(payload, "fresh")
    payload["state"] = "complete"
    write_journal(journal, payload)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", type=Path, required=True)
    subparsers = parser.add_subparsers(dest="command", required=True)
    fence_parser = subparsers.add_parser("fence")
    fence_parser.add_argument("--transition-id", required=True)
    begin_parser = subparsers.add_parser("begin")
    begin_parser.add_argument("--transition-id", required=True)
    begin_parser.add_argument("--active-db", type=Path, default=Path("harness.db"))
    begin_parser.add_argument("--active-log", type=Path, default=Path(".harness/changesets"))
    begin_parser.add_argument("--fresh-db", type=Path, required=True)
    begin_parser.add_argument("--fresh-log", type=Path, required=True)
    begin_parser.add_argument("--archive-root", type=Path, required=True)
    begin_parser.add_argument("--inject-after", choices=("prepared",) + RENAME_STEPS)
    recover = subparsers.add_parser("recover")
    recover.add_argument("--strategy", choices=("forward", "compensate"), required=True)
    completion = subparsers.add_parser("complete")
    completion.add_argument("--transition-id", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.repo_root = args.repo_root.resolve()
    control = args.repo_root / ".harness" / "epoch-transition"
    control.mkdir(parents=True, exist_ok=True)
    lock_path = control / "writer.lock"
    journal = control / "journal.json"
    with lock_path.open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if args.command == "begin":
            if journal.exists():
                previous = read_journal(journal)
                can_promote_fence = (
                    previous["state"] == "fenced"
                    and previous["transition_id"] == args.transition_id
                )
                if previous["state"] not in ("complete", "compensated") and not can_promote_fence:
                    raise TransitionError("an unfinished epoch transition already exists")
            begin(args, journal)
        elif args.command == "fence":
            if journal.exists():
                previous = read_journal(journal)
                if previous["state"] not in ("complete", "compensated"):
                    raise TransitionError("an unfinished epoch transition already exists")
            fence(journal, args.transition_id)
        elif args.command == "recover":
            payload = read_journal(journal)
            if payload["state"] in ("complete", "compensated"):
                raise TransitionError(f"transition is already {payload['state']}")
            if payload["state"] == "fenced":
                if args.strategy != "compensate":
                    raise TransitionError("a fence without a prepared pair can only compensate")
                payload["state"] = "compensated"
                write_journal(journal, payload)
            elif args.strategy == "forward":
                finish_forward(payload, journal)
            else:
                compensate(payload, journal)
        else:
            complete(journal, args.transition_id)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except TransitionError as error:
        print(f"epoch transition error: {error}", file=sys.stderr)
        raise SystemExit(1)
