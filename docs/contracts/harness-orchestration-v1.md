# Harness Orchestration Protocol v1

## Status And Compatibility Floor

This is the public, consumer-neutral process contract for Harness CLI protocol
version `1`. It is additive: existing commands and human-readable output remain
supported. A machine consumer must discover support before any mutation.

The immutable positive release tag is recorded here when US-092 is published:

```text
HARNESS_PROTOCOL_V1_TAG=harness-cli-v0.1.14
```

CLI `0.1.11` and schema `12` are the pre-separation baseline, not protocol-v1
support. Consumers must require the recorded tag (or a later explicitly tested
compatible tag), protocol `1`, schema in the advertised range, and every
capability they use. They must not infer protocol support from semantic version
ordering alone.

## Process And Environment

Invoke `scripts/bin/harness-cli` on macOS/Linux or
`scripts/bin/harness-cli.exe` on Windows. An orchestrator may instead provide an
absolute executable path. The working repository is selected with
`HARNESS_REPO_ROOT`; its database is selected with `HARNESS_DB_PATH`, otherwise
the CLI uses `<repo-root>/harness.db`. The Harness source repository
automatically logs typed default-database mutations; `HARNESS_RUN_ID` supplies
an explicit identity elsewhere. `HARNESS_REQUEST_ID`, when set, is copied into
the response after trimming and limiting it to 128 Unicode scalar values.

Phase 4 freezes human lifecycle writes to the Harness source repository's
default database unless a maintainer explicitly supplies the global
`--compatibility-write` flag. This does not apply to the protocol-v1 `--json`
surface described below, installed consumers, explicit `HARNESS_DB_PATH`
databases, or initialization, migration, replay, snapshot, and read operations.
Protocol consumers do not add the human-maintenance flag.

Arguments are platform-native paths. JSON strings are UTF-8. If a path cannot
be represented in a JSON result, the command fails before mutation with
`PATH_NOT_UTF8`; a consumer must not reinterpret lossy path text. Spaces are
ordinary path characters and never require shell construction when arguments
are passed as an argv array.

## Discovery Before Mutation

Run:

```text
harness-cli query contract --json
```

Discovery is dispatched without automatic database initialization or
migration. It does not create the DB, schema, changeset, trace, or WAL files.
Its result is:

```json
{
  "protocol_version": 1,
  "operation": "query.contract",
  "request_id": "req-123",
  "result": {
    "protocol_version": 1,
    "cli_version": "0.1.12",
    "schema_minimum": 1,
    "schema_maximum": 14,
    "database_state": "current",
    "database_schema_version": 14,
    "required_environment_variables": ["HARNESS_DB_PATH"],
    "capabilities": ["changesets.apply.v1", "work-graph.read.v1"]
  }
}
```

`database_state` has exactly these meanings:

| State | Cause | Consumer action |
| --- | --- | --- |
| `missing` | No database exists at the selected path. | Run an explicit supported initialization flow; do not mutate stories. |
| `current` | Its schema is inside the advertised range and at the current CLI schema. | Capability checks may proceed. |
| `needs_migration` | Its schema is supported but older than the current CLI schema. | Run an explicit migration, then rediscover. |
| `unsupported` | Header/schema is unreadable, newer than supported, or below the supported floor. | Stop and select a compatible CLI or restore/migrate through an approved path. |

Protocol-v1 capabilities are behavioral promises, not product names:

```text
stories.read.v1
stories.write.v1
work-graph.read.v1
story-dependencies.read-write.v1
story-hierarchy.read-write.v1
changesets.apply.v1
changesets.status-sha.v1
entity-revision-conflicts.v1
isolated-db.v1
isolated-db-snapshot.v1
semantic-operation-log.v1
```

Unknown capabilities and unknown additive JSON fields must be ignored. A
missing required capability is a hard compatibility failure before mutation.

## Envelope, Output, And Exit Contract

Every `--json` command writes exactly one newline-terminated UTF-8 JSON document
to stdout. It writes no progress text to stdout. Diagnostics may use stderr and
must not contain secrets. Success has `result` and no `error`; failure has
`error` and no `result`:

```json
{
  "protocol_version": 1,
  "operation": "story.update",
  "request_id": "sync-42",
  "error": {
    "code": "CONFLICT",
    "message": "safe human-readable explanation",
    "retryable": false,
    "details": {}
  }
}
```

The CLI limits its stdout machine document to 16 MiB including the trailing
newline. Consumers must impose a 16 MiB limit on stdout plus stderr combined,
terminate the process tree if exceeded, and treat a truncated/non-JSON response
as an internal protocol failure.

| Exit | Category | Stable codes |
| ---: | --- | --- |
| `0` | Success | none |
| `2` | Invalid input or compatibility | `INVALID_ARGUMENT`, `COMPATIBILITY_ERROR`, `PATH_NOT_UTF8` |
| `3` | Missing object or compare-and-set conflict | `NOT_FOUND`, `CONFLICT` |
| `4` | Verification rejected completion | `VERIFICATION_FAILED` |
| `5` | Internal/resource failure | `OUTPUT_LIMIT_EXCEEDED`, `INTERNAL_ERROR` |

`retryable` is authoritative for ordinary failures. Protocol v1 currently
reports `false`; a future additive error may report `true`. Consumers branch on
`code`, never on `message`. Details are a bounded object and may gain fields.

## Timeouts And Cancellation

The consumer timeout is 30 seconds for discovery/read/status commands and 300
seconds for mutations, changeset apply, initialization/migration, and snapshot.
A deployment may configure a smaller value or a larger value capped at 120
seconds for reads and 900 seconds for mutations. Timeout starts when the child
process is created and includes output collection.

On timeout, output overflow, or caller cancellation, terminate the whole
process tree: send `SIGTERM`, wait at most 5 seconds, then `SIGKILL` on
macOS/Linux; use a Windows Job Object (or equivalent tree termination) on
Windows. Do not merely kill the shell parent.

Read timeouts have no expected logical side effect. A mutation timeout has an
unknown outcome: SQLite may have committed immediately before cancellation.
Therefore rediscover compatibility and query the operation's logical/status
state before retrying. For example, after a changeset timeout, run
`db changeset status <path> --json`; never assume either rollback or success.

## Read Schemas

### Stories

```text
harness-cli query stories --json
```

The result is an array ordered by `id`. Each record has:

```json
{
  "id": "US-092",
  "title": "Machine-Readable Harness Orchestration Contract",
  "risk_lane": "high_risk",
  "contract_doc": "docs/.../overview.md",
  "status": "planned",
  "verify_command": "cargo test --locked",
  "runnable": true
}
```

`runnable` is true exactly when the stored status is `planned`, the trimmed
verification command is non-empty, and every direct dependency blocker is
`implemented`. Hierarchy does not alter runnable state. Consumers use this
field and must not reproduce the SQL rules.

### Consistent Work Graph

```text
harness-cli query work-graph --json
```

The result contains `stories`, `dependencies`, `hierarchy`, and `revision`.
All collections come from one SQLite read transaction. Stories are ordered by
`id`; dependency edges by `(blocker, blocked)`; hierarchy edges by
`(parent, child)`. `revision` is lowercase SHA-256 over the UTF-8 bytes of a
compact JSON object with lexicographically ordered keys `dependencies`,
`hierarchy`, and `stories`, using those ordered collections and no revision
field. It changes exactly when their logical content changes.

Dependency and hierarchy records are:

```json
{"blocker":"US-091","blocked":"US-093"}
{"parent":"US-090","child":"US-091"}
```

Generic callers may use deterministic separate reads:

```text
harness-cli query dependencies [--story <id>] --json
harness-cli query hierarchy [--story <id>] --json
```

An orchestrator making one scheduling decision uses `work-graph`, not three
separate commands whose revisions could differ.

## Mutation Commands

The protocol-v1 machine mutation surface is:

```text
harness-cli story add --id <id> --title <title> --lane <lane> [--contract <path>] [--verify <command>] [--notes <text>] --json
harness-cli story update --id <id> --status <status> --expected-status <status> [--require-runnable] --json
harness-cli story complete <id> --json
harness-cli story dependency add --blocker <id> --blocked <id> --json
harness-cli story dependency remove --blocker <id> --blocked <id> --json
harness-cli story hierarchy add --parent <id> --child <id> --json
harness-cli story hierarchy remove --parent <id> --child <id> --json
```

Add/remove edge operations are idempotent and report whether state changed.
Dependency and hierarchy self-links and cycles fail atomically. When
semantic capture is active, each successful logical mutation emits its
versioned operation; a failed mutation emits none.

For an orchestrator status transition, `--expected-status` compares the stored
status in the same write transaction. `--require-runnable` evaluates the
runnable definition in that transaction. Failure returns `CONFLICT`/exit `3`
and no write. Success returns the story ID, `before_status`, `after_status`, and
`runnable_before`. Example cause and effect: selection observes a Ready story;
another process changes it to `changed`; the later retirement supplies
`--expected-status planned --require-runnable`; the command conflicts instead
of retiring stale work.

`implemented` is not a valid target for either the human-readable `story
update` command or this JSON compare-and-set surface. Such an attempt returns
`INVALID_ARGUMENT`/exit `2`, writes neither story fields nor a semantic
operation, and directs the caller to `story complete <id>`. The required cause
and effect is: move selected work from `planned` to `in_progress`, implement the
change, then invoke `story complete`; only fresh passing completion proof may
create the live `implemented` state. Changeset apply and brownfield import keep
accepting historical implemented records so existing provenance can still be
replayed or migrated.

## Changesets

```text
harness-cli db changeset status <path> --json
harness-cli db changeset apply <path> --json
```

Both parse the JSONL file and return `id`, lowercase byte-exact
`content_sha256`, `applied`, and operation count (`operation_count` for status,
`operations` for apply). Status is read-only. Apply validates the first
`changeset.header` operation, header version `1`, and its
`base_schema_version` compatibility before opening the write transaction.

A previously applied ID with the same SHA is an idempotent skip. The same ID
with different bytes is `CONFLICT`, never a skip. Unsupported/malformed header,
schema, or operation is `COMPATIBILITY_ERROR`. Apply is transactional: either
all semantic operations and its applied marker commit, or none do.

Guarded mutable operations carry `expected_revision`. A mismatch is
`CONFLICT`/exit `3` and rolls back the whole changeset. Its `details` object is:

```json
{
  "changeset_id": "run_auto_...",
  "entity_kind": "story",
  "entity_id": "US-200",
  "expected_revision": 3,
  "actual_revision": 4
}
```

The caller rebases and reruns the high-level mutation after deciding the
correct intent. It must not retry the stale changeset unchanged.

Operations emitted after the reproducible-state cutover also carry database-
generated timestamps used by inserts and time-stamped updates. Replay writes
those recorded values instead of evaluating `datetime('now')` again. Historical
operation versions remain accepted; the verified snapshot compacts their
pre-cutover values.

## WAL-Safe Snapshot

```text
harness-cli db snapshot --output <new-path> --json
```

The output path must not exist. Harness creates a temporary database beside the
requested output, uses SQLite's online backup API (therefore including committed
pages still present only in WAL), integrity-checks it, then atomically renames
it. It never copies `harness.db` as a file and does not change source logical
state. Result fields are:

```json
{
  "output": "/tmp/isolated/harness.db",
  "source_logical_sha256": "...",
  "graph_revision": "...",
  "snapshot_file_sha256": "..."
}
```

`source_logical_sha256` hashes canonical logical user-table state;
`graph_revision` follows the work-graph definition; `snapshot_file_sha256`
hashes the completed snapshot bytes. File hashes can vary after a future
SQLite rewrite even when logical hashes match, so parity checks compare logical
state first.

## Source Core-State Materialization

The upstream source repository tracks `.harness/core-state/harness.db` and
`manifest.json`. The manifest binds the baseline byte hash, baseline logical
hash, schema version, and the exact id/path/content hash of each JSONL file
already represented by that snapshot.

When the default source `harness.db` is absent, bootstrap copies the verified
snapshot to a sibling temporary database. Incorporated JSONL is skipped only
when all three manifest fields match; later files replay in lexical filename
order using the normal transactional apply command. Current schema and core
ownership are checked before an atomic rename. Any mismatch removes the
temporary database and leaves the requested output absent. Explicit database
paths and installed consumers retain their existing local-state behavior.

## Installer Upgrade Contract

Normal `--merge`/`-Merge` is not an upgrade: an existing CLI remains untouched.
An explicit forced upgrade uses one immutable release tuple:

```bash
install-harness.sh --merge --upgrade-cli --ref harness-cli-vX.Y.Z --yes
```

```powershell
install-harness.ps1 -Merge -UpgradeCli -Ref harness-cli-vX.Y.Z -Yes
```

The ref must match `harness-cli-v<major>.<minor>.<patch>` (an immutable
prerelease suffix is allowed). The installer downloads template files from
that Git ref and the platform CLI plus `.sha256` from the release with the same
tag. It verifies SHA-256 before touching the installed executable, writes the
candidate on the target filesystem, backs up the old executable, and atomically
renames/replaces it. Download, checksum, or ref validation failure leaves the
old executable runnable. Test-only mirror URLs may be supplied with
`HARNESS_SOURCE_BASE_URL` and `HARNESS_CLI_BASE_URL`; they do not change the
declared ref identity.

## Forward And Breaking Changes

Consumers must tolerate unknown object fields, capabilities, and error-detail
fields, but not an unknown `protocol_version`, missing required field, changed
field type, or undocumented exit/code pairing. Additive fields and capabilities
may ship under v1. Removing/renaming a field, changing ordering/hash semantics,
weakening atomicity, or changing a command's meaning requires a new protocol
version. A published version is deprecated by capability/release documentation,
never silently removed.
