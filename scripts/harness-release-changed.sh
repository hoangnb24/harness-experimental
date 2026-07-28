#!/usr/bin/env bash
set -euo pipefail

# Exit successfully only when a changed path can alter the core-maintenance
# binary, its embedded payload, bootstrap contract, or release proof.
pattern='^('
pattern+='\.agents/skills/(onboard-repository|audit-onboarding-proposal|improve-harness)/|'
pattern+='crates/harness/|Cargo\.toml$|Cargo\.lock$|'
pattern+='docs/(WORKFLOW|README)\.md$|docs/product/README\.md$|'
pattern+='docs/plans/active/README\.md$|docs/templates/(application-runbook|decision|exec-plan|harness-improvement)\.md$|'
pattern+='scripts/(agent-harness-block\.md|harness-install-files\.txt|harness-release-tag)$|'
pattern+='scripts/(install-harness|build-harness-release|harness-release-changed|promote-harness-release-tag|verify-harness-release-assets|verify-harness-release-identity)\.(sh|ps1)$|'
pattern+='\.github/workflows/(harness-release|post-merge-maintenance)\.yml$|'
pattern+='tests/installer/test-install-harness-modes\.(sh|ps1)$|'
pattern+='tests/maintenance/test-harness-release-classification\.sh$'
pattern+=')'
grep -Eq "$pattern"
