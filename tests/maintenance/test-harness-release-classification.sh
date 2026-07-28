#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
classifier="$root/scripts/harness-release-changed.sh"
workflow="$root/.github/workflows/post-merge-maintenance.yml"

expected_triggering_paths=(
  crates/harness/src/main.rs
  crates/harness/assets/docs/plans/README.md
  .agents/skills/onboard-repository/SKILL.md
  .agents/skills/audit-onboarding-proposal/scripts/validate_evidence_capsule.py
  .agents/skills/improve-harness/SKILL.md
  docs/WORKFLOW.md
  docs/templates/application-runbook.md
  docs/templates/harness-improvement.md
  scripts/agent-harness-block.md
  scripts/install-harness.sh
  scripts/install-harness.ps1
  scripts/build-harness-release.sh
  scripts/harness-release-changed.sh
  scripts/promote-harness-release-tag.sh
  scripts/verify-harness-release-assets.sh
  scripts/verify-harness-release-identity.sh
  .github/workflows/harness-release.yml
  .github/workflows/post-merge-maintenance.yml
  Cargo.toml
  Cargo.lock
)

for path in "${expected_triggering_paths[@]}"; do
  if ! printf '%s\n' "$path" | "$classifier" >/dev/null; then
    echo "expected path did not trigger Harness core publication: $path" >&2
    exit 1
  fi
done

for unrelated in crates/harness-cli/src/main.rs docs/HARNESS.md README.md; do
  if printf '%s\n' "$unrelated" | "$classifier" >/dev/null; then
    echo "unrelated path triggered Harness core publication: $unrelated" >&2
    exit 1
  fi
done

grep -Fq 'scripts/harness-release-changed.sh <<<"$changed_files"' "$workflow"
echo "Harness core post-merge release classification tests passed"
