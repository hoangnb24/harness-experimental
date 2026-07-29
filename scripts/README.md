# Scripts

This directory contains repository validation, installation, and optional
compatibility-control-plane tools.

The default workflow is defined by `AGENTS.md` and `docs/WORKFLOW.md`. Normal
questions and repository changes do not require a database, bootstrap, intake,
story row, matrix query, trace, score, audit, or proposal. Use the CLI described
below only when a user or external orchestrator explicitly selects the legacy
SQLite contract.

## Core Maintenance CLI

The default installer places `harness` at `scripts/bin/harness` on macOS/Linux
or `scripts/bin/harness.exe` on Windows. It owns only core installation,
three-way updates, provenance, status, and diagnostics:

```bash
scripts/bin/harness update --dry-run
scripts/bin/harness update
scripts/bin/harness update --continue --dry-run
scripts/bin/harness update --continue
scripts/bin/harness update --abort
scripts/bin/harness status
scripts/bin/harness doctor
```

`update` resolves the published core-release pointer, downloads that exact
versioned release candidate and SHA-256 sidecar, requires the tag and binary
versions to agree, then delegates preview or application to the verified
candidate. Overlapping local/upstream edits create an ignored
`.harness-core/update/` resolution packet without changing live managed files.
After an agent obtains human direction and edits the staged RESOLVED copy, use
`--continue`; use `--abort` to discard the session. Running normal `update`
instead abandons the pending plan and replans a direct jump from the installed
version to the latest release; dry-run previews that jump without discarding
the session.

The platform installers bootstrap a checksum-verified immutable artifact and
delegate core semantics to this binary. They run an update candidate before
replacing an existing executable. A conflict retains the candidate under
`.harness-core/update-candidate/`; resolve the packet and rerun the installer to
continue the pending version and replace the binary only after success.
Repository executable and retained-candidate paths must not be symlinks.

## Compatibility Harness CLI

The Rust `harness-cli` is the primary interface for the optional SQLite durable
layer. Installed projects receive the prebuilt binary at
`scripts/bin/harness-cli` on macOS/Linux or `scripts/bin/harness-cli.exe` on
Windows so compatibility consumers retain a stable command path.

Request authority comes before runtime setup. Answer, explain, review,
diagnose, plan, and status requests remain read-only: inspect what is already
present, and do not bootstrap, initialize/migrate, record intake, or trace.

When the compatibility control plane has been explicitly selected, bootstrap
its local ignored runtime before querying or changing that state:

```bash
scripts/bootstrap-harness.sh
```

```powershell
.\scripts\bootstrap-harness.ps1
```

In this source repository, bootstrap builds the CLI from the checked-out Rust
source so code and command behavior cannot drift. When the default database is
missing, it verifies `.harness/core-state/manifest.json` and the read-only
snapshot, copies the snapshot to a temporary database, skips only the exact
JSONL files named by the manifest, replays later changesets, verifies ownership,
and atomically installs the result. A changed snapshot or compacted changeset
fails without leaving `harness.db`. In an installed consumer bootstrap uses the
checksum-verified prebuilt binary and safely initializes missing local state.
Both modes migrate older supported databases and refuse unsupported schemas or
CLI/release-pin drift.

Human lifecycle writes to this source repository's default `harness.db` are
frozen for new work. Use Git-native plans and decisions instead. Only deliberate
maintenance of preserved compatibility state may add the global
`--compatibility-write` flag, for example
`scripts/bin/harness-cli --compatibility-write intake ...`. Installed
consumers, protocol-v1 JSON operations, and explicit `HARNESS_DB_PATH` workflows
retain their existing command shapes.

```bash
scripts/bin/harness-cli init          # Create the database
scripts/bin/harness-cli intake ...    # Record a feature intake classification
scripts/bin/harness-cli story ...     # Add or update a story (test matrix row)
scripts/bin/harness-cli story update --id US-001 --unit 1 --integration 1 --e2e 0 --platform 0
scripts/bin/harness-cli story verify US-001  # Run the story's verify_command
scripts/bin/harness-cli story complete US-001 # Fresh proof plus atomic lifecycle completion
scripts/bin/harness-cli decision ...  # Add a decision or run its verification
scripts/bin/harness-cli backlog ...   # Add or close a backlog item
scripts/bin/harness-cli propose       # Classify new, active, handled, and recurring evidence
scripts/bin/harness-cli propose --show-suppressed # Explain handled evidence
scripts/bin/harness-cli trace ...     # Record and auto-score an agent execution trace
scripts/bin/harness-cli score-trace   # Score a trace against TRACE_SPEC.md tiers
scripts/bin/harness-cli query ...     # Query harness data, including backlog --open/--closed
scripts/bin/harness-cli query matrix --numeric  # Show proof flags as 1/0
scripts/bin/harness-cli query matrix --active --summary  # Focus on unfinished work without evidence text
scripts/bin/harness-cli query matrix --runnable --summary # Show work ready under protocol-v1 rules
scripts/bin/harness-cli query matrix --story US-001      # Inspect one exact story
scripts/bin/harness-cli db changeset apply .harness/changesets/run_123.changeset.jsonl
scripts/bin/harness-cli db rebuild --from .harness/changesets
scripts/materialize-core-state.sh   # Restore a missing source harness.db
scripts/verify-core-snapshot.sh     # Verify the tracked baseline tuple
scripts/bin/harness-cli migrate       # Apply pending schema migrations
scripts/bin/harness-cli --version     # Print the installed CLI version
```

Run `scripts/bin/harness-cli help` or `scripts/bin/harness-cli query help` for
full usage. On Windows, use the same commands through
`.\scripts\bin\harness-cli.exe`.

Proof flags on `story update` are numeric booleans: use `1` for yes and `0` for
no. `story verify <id>` runs the configured `verify_command`; it does not accept
proof flags. Configure the command with `story add/update --verify`, run
`story verify <id>`, then update proof flags with `story update`.

`story update --status implemented` is rejected in both human-readable and
JSON/CAS modes. Move active work to `in_progress` or `changed`, then use `story
complete <id>` so fresh proof and the implemented transition happen together.

Backlog `--risk` uses Harness lanes, not severity words: use `tiny`, `normal`,
or `high-risk`. Use `tiny` instead of `low`. `query matrix` defaults to
human-readable `yes`/`no`; use `query matrix --numeric` when copying values into
`story update`. Matrix filters combine with AND semantics: `--active` keeps
`planned`, `in_progress`, and `changed` stories; `--runnable` uses the same rule
as protocol story discovery; `--story <id>` selects one exact ID. `--summary`
omits the potentially long evidence column while keeping lane and runnable
state. With none of these flags, the existing full matrix output is unchanged.

The schema lives in `scripts/schema/` and is version-controlled. The database
file (`harness.db`) is `.gitignore`d.

Repository maintainers can run `scripts/verify-revision-coherence.sh` to prove
that crate, lockfile, pinned-release, migration, protocol, bootstrap, replay,
and public-command contracts describe the same revision.

Set `HARNESS_DB_PATH=/path/to/harness.db` when a workflow needs `harness-cli`
to operate on an isolated copied database. `HARNESS_DB_PATH` takes precedence
over the legacy `HARNESS_DB` override; if neither is set, the CLI uses
`harness.db` in the repository root.

In this Harness CLI source repository, an explicitly authorized typed write to
the default `harness.db` automatically records semantic operations in one
uniquely named `.harness/changesets/run_auto_*.changeset.jsonl` file per CLI
invocation. Human maintenance supplies `--compatibility-write`; protocol-v1
machine operations remain authorized by their JSON contract. The caller does
not need to begin or finish a run. The first line is a `changeset.header`;
later lines are typed operations such as `story.update`, `trace.add`, and
`decision.add`.

Set `HARNESS_RUN_ID=<run-id>` to supply an explicit identity or aggregate the
operations from several invocations into one run file. Installed consumers and
isolated `HARNESS_DB_PATH` workflows remain opt-in: without `HARNESS_RUN_ID`,
they write only their local operational database and do not create a changeset.

Mutable operations record the entity revision they observed. If replay finds a
different revision, `db changeset apply` stops the entire changeset and reports
the run ID, entity kind and ID, expected revision, and actual revision. Resolve
the branch intent, rebase, and rerun the normal domain command; do not edit a
shared changeset or blindly retry it.

Snapshot publication is an explicit maintenance operation, never part of a
normal task. `scripts/publish-core-snapshot.sh` refuses an existing tuple,
requires ownership-clean source state, creates the database through SQLite
online backup, scans it for machine paths and token-shaped secrets, and binds
all incorporated JSONL ids and hashes in the manifest.

Infrequent compaction uses compare-and-swap against the manifest the maintainer
reviewed:

```bash
current=$(jq -r '.snapshot.logical_sha256' .harness/core-state/manifest.json)
scripts/publish-core-snapshot.sh --replace --expected-logical-sha "$current"
```

Replacement verifies the old pair and the candidate pair before activation and
restores the prior pair after an in-process failure. A stale expected hash makes
no change. It deliberately retains incorporated JSONL; deleting history needs a
separate retention decision. Pull-request CI begins without `harness.db`,
bootstraps the tracked tuple on Linux and Windows, and runs materialized parity.

Requires: the prebuilt Rust CLI at `scripts/bin/harness-cli` on macOS/Linux or
`scripts/bin/harness-cli.exe` on Windows.

Direct database inspection may still use SQLite tools, but normal Harness use
should go through the Rust CLI.

### Rust CLI Commands

Current migrated commands:

```bash
scripts/bin/harness-cli init
scripts/bin/harness-cli migrate
scripts/bin/harness-cli import brownfield
scripts/bin/harness-cli intake ...
scripts/bin/harness-cli story add ...
scripts/bin/harness-cli story update ...
scripts/bin/harness-cli story verify ...
scripts/bin/harness-cli story complete ...
scripts/bin/harness-cli decision add ...
scripts/bin/harness-cli decision verify ...
scripts/bin/harness-cli backlog add ...
scripts/bin/harness-cli backlog close ...
scripts/bin/harness-cli trace ...
scripts/bin/harness-cli score-trace
scripts/bin/harness-cli query matrix
scripts/bin/harness-cli query backlog
scripts/bin/harness-cli query decisions
scripts/bin/harness-cli query intakes
scripts/bin/harness-cli query traces
scripts/bin/harness-cli query friction
scripts/bin/harness-cli query stats
scripts/bin/harness-cli query sql ...
scripts/bin/harness-cli db changeset apply ...
scripts/bin/harness-cli db rebuild --from ...
```

`query sql` accepts one read-only SQLite statement. The CLI enforces read-only
access at the database connection, including for CTEs, pragmas, and statements
with `RETURNING`; use typed Harness mutation commands, migrations, or semantic
changesets for writes.

`scripts/bin/harness-cli import brownfield` seeds or refreshes the durable database
from existing Harness v0 markdown in `docs/TEST_MATRIX.md`,
`docs/decisions/`, and `docs/HARNESS_BACKLOG.md`. This keeps already-installed
Harness repos on the Rust CLI path without losing their populated operating
docs.

## Installer

The upstream installer applies the Harness v0 operating files and folder
structure to a target project directory. It defaults to the current directory,
accepts a target path, and asks interactive users whether to `1. Merge`,
`2. Override`, or `3. Stop` when the target already contains `AGENTS.md`,
`docs/`, or `scripts/`.
Non-interactive installs stop on those protected paths unless `--merge` or
`--override` is provided. Use `--merge` as the safe update path for repositories
that already have Harness: it keeps existing files in place and creates only
missing Harness files. Add `--refresh-agent-shim` when an older install has the
full generated Harness guide in `AGENTS.md` and should move to the small stable
shim. Use `--override` only when replacing the protected Harness surface is
intentional.

```bash
curl -fsSL "https://raw.githubusercontent.com/hoangnb24/repository-harness/main/scripts/install-harness.sh?$(date +%s)" | bash -s -- --yes
```

```powershell
& ([scriptblock]::Create((irm "https://raw.githubusercontent.com/hoangnb24/repository-harness/main/scripts/install-harness.ps1"))) -Yes
```

```bash
curl -fsSL "https://raw.githubusercontent.com/hoangnb24/repository-harness/main/scripts/install-harness.sh?$(date +%s)" | bash -s -- --merge --yes
```

```powershell
& ([scriptblock]::Create((irm "https://raw.githubusercontent.com/hoangnb24/repository-harness/main/scripts/install-harness.ps1"))) -Merge -Yes
```

```bash
curl -fsSL "https://raw.githubusercontent.com/hoangnb24/repository-harness/main/scripts/install-harness.sh?$(date +%s)" | bash -s -- --merge --refresh-agent-shim --yes
```

```powershell
& ([scriptblock]::Create((irm "https://raw.githubusercontent.com/hoangnb24/repository-harness/main/scripts/install-harness.ps1"))) -Merge -RefreshAgentShim -Yes
```

`--refresh-agent-shim` backs up `AGENTS.md` before changing it. If the existing
file is recognized as the old Harness-generated operating guide, the installer
replaces it with the current shim. Otherwise it appends or replaces only the
marked `<!-- HARNESS:BEGIN -->` block so project-specific instructions remain
in place. Both installers read that block from
`scripts/agent-harness-block.md`; the Bash `--claude` path reads
`scripts/claude-harness-block.md`, whose only import is `AGENTS.md`. This keeps
root, fresh-install, and refresh behavior on the same authority text.

`--upgrade-cli` (PowerShell: `-UpgradeCli`) also refreshes the marked
`AGENTS.md` Harness block. This prevents a new binary from retaining stale
request authority while preserving text outside the marked block and creating
the normal backup. It does not overwrite arbitrary custom documentation.

The installer must stay limited to harness files. Do not use it to scaffold
application source folders, package scripts, CI, tests, platform shells, or fake
validation commands. The installer script is not part of the installed project
payload.

The default core payload is declared in
`scripts/harness-install-files.txt`. The optional static CLI payload is declared
in `scripts/harness-cli-install-files.txt`. Both platform installers read both
manifests; do not duplicate file lists in installer code. Schema migrations are
members of the CLI profile and are discovered from `scripts/schema/*.sql`, so a
new migration requires only the SQL file.

The default manifest includes the explicit-only `$onboard-repository` and
`$audit-onboarding-proposal` skill packages. They are embedded and updated as
ordinary core files; the installer never invokes them or installs their runtime
dependencies.

By default the installer downloads the checksum-verified `harness` maintenance
binary and delegates installation of the repository-centered core. It performs
no compatibility-CLI download, schema discovery, database bootstrap, or
database ignore write. Select `--with-cli` (PowerShell: `-WithCli`) to stage the
complete compatibility bundle and download the prebuilt Rust executable into
`scripts/bin/harness-cli` on macOS/Linux or `scripts/bin/harness-cli.exe` on
Windows after verifying its `.sha256` checksum.

A source branch can pin the release used by the installer through
`scripts/harness-cli-release-tag`; the tracked pin currently reads
`harness-cli-v0.1.23` so branch installs receive the latest released CLI. Set
`HARNESS_CLI_RELEASE_TAG` to override that tag, or set `HARNESS_CLI_BASE_URL` to
point at an alternate artifact directory, such as a local `file:///.../dist`
directory created by `scripts/build-harness-cli-release.sh`.

`--merge` (PowerShell: `-Merge`) deliberately does not replace an existing CLI.
An ordinary core merge or override does not touch the scripts tree at all.
To upgrade a CLI explicitly, pin one immutable tag for both the template files
and release artifact:

```bash
curl -fsSL "https://raw.githubusercontent.com/hoangnb24/repository-harness/harness-cli-v0.1.14/scripts/install-harness.sh" |
  bash -s -- --merge --upgrade-cli --ref harness-cli-v0.1.14 --yes
```

```powershell
& ([scriptblock]::Create((irm "https://raw.githubusercontent.com/hoangnb24/repository-harness/harness-cli-v0.1.14/scripts/install-harness.ps1"))) `
  -Merge -UpgradeCli -Ref harness-cli-v0.1.14 -Yes
```

The installer rejects branch names and `latest`, downloads the tagged template
and matching platform artifact, verifies the published SHA-256, backs up the
old executable, then atomically replaces it. A download/checksum failure leaves
the old CLI in place. Run `scripts/test-install-harness-cli-upgrade.sh` for the
merge/upgrade/checksum-preservation regression.

The external process protocol, including JSON envelopes, capability discovery,
timeouts, and snapshot semantics, is documented in
`docs/contracts/harness-orchestration-v1.md`.

## Schema Migrations

Migration files live under `scripts/schema/` and are named `NNN-description.sql`
where `NNN` is a zero-padded version number. Run `scripts/bin/harness-cli migrate` to
apply pending migrations.

## Pre-Merge Validation Contract

Repository maintainers and pull-request CI run the same release-relevant gate:

```bash
scripts/validate-premerge.sh
```

It checks Rust formatting, tests, and linting; revision/schema/command
coherence; bootstrap, protocol, installer, documentation, and representative
task-effect contracts; release workflow structure; and whitespace errors.
Installed consumer projects keep their own stack-specific validation commands;
the template does not impose this repository's Rust gate on them.

## Changeset Rebuild Validation

Run the durable repository rebuild and its validator contract regressions:

```bash
scripts/validate-changeset-rebuild.sh
scripts/test-validate-changeset-rebuild.sh
```

The validator builds the current workspace CLI unless `HARNESS_CLI` is set
explicitly. `HARNESS_CHANGESET_DIR` can point validation at a copied fixture
history without changing the repository changesets.

## Release Packaging

Build the current-platform core-maintenance artifact with:

```bash
scripts/build-harness-release.sh
```

It writes `dist/harness-<platform>` plus a checksum. The reusable
`.github/workflows/harness-release.yml` proves all five native platforms,
promotes an annotated immutable `harness-v*` tag, and publishes ten assets.
Changes that affect the crate, embedded core files, installers, or release proof
automatically bump and publish the next patch after merge.

Build the current-platform Rust CLI release artifact from the source repo:

```bash
scripts/build-harness-cli-release.sh
```

The script writes `dist/harness-cli-<platform>` plus `.sha256` checksums. The
Windows artifact includes the `.exe` suffix. Supported labels are:

- `macos-arm64`
- `macos-x64`
- `linux-x64`
- `linux-arm64`
- `windows-x64`

For cross-compilation, pass a Cargo target triple:

```bash
scripts/build-harness-cli-release.sh --target x86_64-unknown-linux-gnu
```

GitHub releases are produced by
`.github/workflows/harness-cli-release.yml`. Post-merge automation or an
explicit manual dispatch supplies a desired `harness-cli-v*` tag and an exact
main-branch source ref. The workflow verifies the untagged candidate, builds all
supported targets on native hosted runners, and creates the annotated tag only
after every platform and upgrade-transition check passes. It then publishes
these release assets without overwriting an existing release:

- `harness-cli-macos-arm64`
- `harness-cli-macos-arm64.sha256`
- `harness-cli-macos-x64`
- `harness-cli-macos-x64.sha256`
- `harness-cli-linux-x64`
- `harness-cli-linux-x64.sha256`
- `harness-cli-linux-arm64`
- `harness-cli-linux-arm64.sha256`
- `harness-cli-windows-x64.exe`
- `harness-cli-windows-x64.exe.sha256`

Merged PRs are handled by `.github/workflows/post-merge-maintenance.yml`. The
workflow always prepends a PR summary to `CHANGELOG.md`. It independently
classifies changes for `harness` and `harness-cli`, increments the affected
patch versions, updates their release pins, and calls each reusable workflow
with the exact maintenance commit. The old `harness-cli-v0.1.14` upgrade source
is checked against a frozen historical contract; built candidates are checked
against current contracts. A failed tag is consumed and immutable, so recovery
advances to a later patch version.
