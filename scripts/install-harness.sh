#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: install-harness.sh [options] [path]

Bootstrap the Rust `harness` CLI and install the Harness core into a target.

Options:
  -d, --directory <path>  Target directory. Defaults to the current directory.
  -y, --yes              Accept defaults and skip prompts.
      --with-cli         Add the optional CLI compatibility bundle: lifecycle
                         docs, bootstrap scripts, schemas, ignore rules, and a
                         checksum-verified platform binary.
      --with-engineering-wisdom
                         Add the explicit-only engineering-wisdom advisory
                         skill. It is excluded from the default core.
      --merge            On protected-path conflict, keep existing files in
                         place and install only missing Harness files.
      --upgrade-cli      Add the CLI bundle, replace the installed CLI after
                         checksum verification, and refresh the marked
                         AGENTS.md authority block. Requires --ref.
      --ref <tag>        Immutable Harness release tag used for both template
                         files and the CLI artifact (harness-cli-vX.Y.Z).
      --refresh-agent-shim
                         Refresh an existing AGENTS.md into the small Harness
                         shim after backing it up. Old Harness-generated files
                         are replaced; custom files receive a marked block.
      --claude           Also install or refresh CLAUDE.md so Claude Code
                         auto-loads the harness context. Claude Code never
                         auto-loads AGENTS.md; the shim @-imports AGENTS.md
                         as its single policy source inside a marked block.
                         Existing CLAUDE.md files get the block appended
                         after a backup; a stale block is refreshed in place.
      --override         On protected-path conflict, back up and replace
                         AGENTS.md, docs/, and scripts/.
      --force            Overwrite existing files after backing them up.
      --dry-run          Show what would change without writing files.
  -h, --help             Show this help.

Safety:
  The default profile installs the repository-centered core plus the Rust
  maintenance CLI. It performs no SQLite/control-plane download or database
  write. If AGENTS.md, docs/, or scripts/
  already exist, interactive installs ask
  whether to merge missing files, override after backup, or stop. Merge is the
  safe update path for repositories that already have Harness: existing files
  stay in place and new Harness files are appended by path. Non-
  interactive installs stop unless --merge or --override is provided. If a
  target .gitignore is changed only when --with-cli or --upgrade-cli selects
  the compatibility bundle.

Examples:
  scripts/install-harness.sh
  scripts/install-harness.sh --directory /path/to/project --yes
  scripts/install-harness.sh --directory /path/to/project --with-cli --yes
  scripts/install-harness.sh --directory /path/to/project --with-engineering-wisdom --yes
  scripts/install-harness.sh ./my-project --force
  curl -fsSL https://raw.githubusercontent.com/hoangnb24/repository-harness/main/scripts/install-harness.sh | bash -s -- --yes
  curl -fsSL https://raw.githubusercontent.com/hoangnb24/repository-harness/main/scripts/install-harness.sh | bash -s -- --merge --yes
  curl -fsSL https://raw.githubusercontent.com/hoangnb24/repository-harness/harness-cli-v0.1.14/scripts/install-harness.sh | bash -s -- --merge --upgrade-cli --ref harness-cli-v0.1.14 --yes
  curl -fsSL https://raw.githubusercontent.com/hoangnb24/repository-harness/main/scripts/install-harness.sh | bash -s -- --merge --refresh-agent-shim --yes
  curl -fsSL https://raw.githubusercontent.com/hoangnb24/repository-harness/main/scripts/install-harness.sh | bash -s -- --claude --yes
EOF
}

log() {
  printf '%s\n' "$*"
}

fail() {
  printf 'Error: %s\n' "$*" >&2
  exit 1
}

warn_stop() {
  printf 'Warning: %s\n' "$*" >&2
  exit 1
}

can_prompt() {
  [ -r /dev/tty ] && [ -w /dev/tty ]
}

prompt_tty() {
  printf '%s' "$1" > /dev/tty
}

read_tty() {
  local value
  IFS= read -r value < /dev/tty
  printf '%s\n' "$value"
}

expand_path() {
  case "$1" in
    "~")
      printf '%s\n' "$HOME"
      ;;
    "~/"*)
      printf '%s/%s\n' "$HOME" "${1#~/}"
      ;;
    /*)
      printf '%s\n' "$1"
      ;;
    *)
      printf '%s/%s\n' "$PWD" "$1"
      ;;
  esac
}

make_absolute_parent() {
  local path="$1"
  local parent
  parent="$(dirname "$path")"
  [ -d "$parent" ] || fail "Parent directory does not exist: $parent"
  (cd "$parent" && printf '%s/%s\n' "$(pwd -P)" "$(basename "$path")")
}

copy_file() {
  local relative="$1"
  local target="$TARGET_DIR/$relative"

  if [ "$relative" = ".gitignore" ] && [ -e "$target" ] && [ "$FORCE" -eq 0 ]; then
    merge_gitignore "$target"
    return
  fi

  if [ -e "$target" ]; then
    if [ "$SOURCE_MODE" = "local" ] && [ "$SOURCE_ROOT/$relative" -ef "$target" ]; then
      log "skip     $relative (source file)"
      SKIPPED=$((SKIPPED + 1))
      return
    fi

    if [ "$CONFLICT_ACTION" = "merge" ]; then
      log "skip     $relative (merge keeps existing file)"
      SKIPPED=$((SKIPPED + 1))
    elif [ "$FORCE" -eq 1 ]; then
      if [ "$DRY_RUN" -eq 1 ]; then
        log "overwrite $relative (backup first)"
      else
        local backup="$BACKUP_DIR/$relative"
        mkdir -p "$(dirname "$backup")"
        cp -p "$target" "$backup"
        write_source_file "$relative" "$target"
        log "updated $relative (backup: ${backup#$TARGET_DIR/})"
      fi
      UPDATED=$((UPDATED + 1))
    else
      log "skip     $relative (already exists)"
      SKIPPED=$((SKIPPED + 1))
    fi
    return
  fi

  if [ "$DRY_RUN" -eq 1 ]; then
    log "create   $relative"
  else
    mkdir -p "$(dirname "$target")"
    write_source_file "$relative" "$target"
    log "created  $relative"
  fi
  CREATED=$((CREATED + 1))
}

merge_gitignore() {
  local target="$1"
  local marker="# Harness durable layer"
  local rules="harness.db
harness.db-wal
harness.db-shm
scripts/bin/harness-cli
scripts/bin/harness-cli.exe"

if [ -f "$target" ] &&
   grep -Fxq "harness.db" "$target" &&
   grep -Fxq "harness.db-wal" "$target" &&
   grep -Fxq "harness.db-shm" "$target" &&
   grep -Fxq "scripts/bin/harness-cli" "$target" &&
   grep -Fxq "scripts/bin/harness-cli.exe" "$target"; then
    log "skip     .gitignore (harness rules already present)"
    SKIPPED=$((SKIPPED + 1))
    return
  fi

  if [ "$DRY_RUN" -eq 1 ]; then
    log "update   .gitignore (append harness rules)"
  else
    {
      [ -s "$target" ] && printf '\n'
      printf '%s\n%s\n' "$marker" "$rules"
    } >> "$target"
    log "updated  .gitignore (appended harness rules)"
  fi
  UPDATED=$((UPDATED + 1))
}

write_source_file() {
  local relative="$1"
  local target="$2"

  if [ "$relative" = "AGENTS.md" ]; then
    {
      printf '# Agent Instructions\n\n'
      agent_shim_block
    } > "$target"
    return
  fi

  if [ "$SOURCE_MODE" = "local" ]; then
    local source="$SOURCE_ROOT/$relative"
    [ -f "$source" ] || fail "Source file missing: $source"
    cp -p "$source" "$target"
    return
  fi

  local url="$SOURCE_BASE_URL/$relative"
  curl -fsSL "$url" -o "$target" || fail "Could not download $url"
}

read_source_text() {
  local relative="$1"

  if [ "$SOURCE_MODE" = "local" ]; then
    local source="$SOURCE_ROOT/$relative"
    [ -f "$source" ] || fail "Source file missing: $source"
    cat "$source"
    return
  fi

  local url="$SOURCE_BASE_URL/$relative"
  curl -fsSL "$url" || fail "Could not download $url"
}

read_payload_manifest() {
  local payload_manifest="$1"
  if [ "$SOURCE_MODE" = "local" ]; then
    local manifest="$SOURCE_ROOT/$payload_manifest"
    [ -f "$manifest" ] || fail "Payload manifest missing: $manifest"
    cat "$manifest"
    return
  fi

  local url="$SOURCE_BASE_URL/$payload_manifest"
  curl -fsSL "$url" || fail "Could not download $url"
}

discover_schema_files() {
  if [ "$SOURCE_MODE" = "local" ]; then
    local schema_root="$SOURCE_ROOT/$SCHEMA_DIR"
    [ -d "$schema_root" ] || fail "Schema directory missing: $schema_root"
    find "$schema_root" -maxdepth 1 -type f -name '*.sql' -print |
      while IFS= read -r path; do
        printf '%s/%s\n' "$SCHEMA_DIR" "$(basename "$path")"
      done |
      sort
    return
  fi

  case "$SOURCE_BASE_URL" in
    file://*)
      local source_root="${SOURCE_BASE_URL#file://}"
      local schema_root="$source_root/$SCHEMA_DIR"
      [ -d "$schema_root" ] || fail "Schema directory missing: $schema_root"
      find "$schema_root" -maxdepth 1 -type f -name '*.sql' -print |
        while IFS= read -r path; do
          printf '%s/%s\n' "$SCHEMA_DIR" "$(basename "$path")"
        done |
        sort
      ;;
    https://raw.githubusercontent.com/*)
      local raw_path="${SOURCE_BASE_URL#https://raw.githubusercontent.com/}"
      local owner repo ref api_url
      IFS=/ read -r owner repo ref _rest <<EOF
$raw_path
EOF
      [ -n "${owner:-}" ] && [ -n "${repo:-}" ] && [ -n "${ref:-}" ] ||
        fail "Cannot infer GitHub repository from $SOURCE_BASE_URL"
      api_url="https://api.github.com/repos/$owner/$repo/git/trees/$ref?recursive=1"
      curl -fsSL "$api_url" |
        sed -n "s#.*\"path\": \"\\($SCHEMA_DIR/[^\"]*\\.sql\\)\".*#\\1#p" |
        sort
      ;;
    *)
      fail "Cannot discover remote schema files from $SOURCE_BASE_URL. Use a local source, file:// source, or raw.githubusercontent.com source."
      ;;
  esac
}

copy_manifest_files() {
  local payload_manifest="$1"
  local manifest
  local relative

  manifest="$(read_payload_manifest "$payload_manifest")"
  while IFS= read -r relative || [ -n "$relative" ]; do
    relative="${relative%$'\r'}"
    case "$relative" in
      ""|\#*)
        continue
        ;;
    esac
    copy_file "$relative"
  done <<EOF
$manifest
EOF
}

agent_shim_block() {
  read_source_text "scripts/agent-harness-block.md"
}

claude_shim_block() {
  read_source_text "scripts/claude-harness-block.md"
}

is_old_harness_agent_file() {
  local target="$1"

  grep -Fxq "# Agent Operating Guide" "$target" &&
    grep -Fxq "This repository is in Harness v0. There is no product implementation yet." "$target" &&
    grep -Fxq "## Source Of Truth" "$target" &&
    grep -Fxq "## Task Loop" "$target" &&
    grep -Fxq "## Done Definition" "$target"
}

backup_agent_file() {
  local target="$TARGET_DIR/AGENTS.md"

  [ -e "$target" ] || return 0
  mkdir -p "$BACKUP_DIR"
  [ -e "$BACKUP_DIR/AGENTS.md" ] && return 0
  cp -p "$target" "$BACKUP_DIR/AGENTS.md"
}

extract_obvious_agent_custom_section() {
  local target="$1"
  local output="$2"

  awk '
    /^## (Project-specific|Project Specific|Local|Custom).*Instructions/ {
      capture = 1
      print
      next
    }
    /^## / && capture {
      capture = 0
    }
    capture {
      print
    }
  ' "$target" > "$output"
}

insert_agent_custom_section() {
  local target="$1"
  local custom="$2"
  local tmp

  [ -s "$custom" ] || return 0
  tmp="$(mktemp)"
  awk '
    $0 == "Add project-specific agent instructions here." {
      while ((getline line < custom_file) > 0) {
        print line
      }
      inserted = 1
      next
    }
    { print }
    END {
      if (!inserted) {
        print ""
        while ((getline line < custom_file) > 0) {
          print line
        }
      }
    }
  ' custom_file="$custom" "$target" > "$tmp"
  mv "$tmp" "$target"
}

append_or_replace_agent_harness_block() {
  local target="$TARGET_DIR/AGENTS.md"
  local block_tmp tmp

  block_tmp="$(mktemp)"
  agent_shim_block >"$block_tmp"
  [ -s "$block_tmp" ] || fail "canonical AGENTS.md Harness block is empty"
  tmp="$(mktemp)"
  if grep -Fq "<!-- HARNESS:BEGIN -->" "$target" &&
     grep -Fq "<!-- HARNESS:END -->" "$target"; then
    awk '
      /<!-- HARNESS:BEGIN -->/ {
        while ((getline line < block_file) > 0) {
          print line
        }
        in_block = 1
        next
      }
      /<!-- HARNESS:END -->/ && in_block {
        in_block = 0
        next
      }
      !in_block { print }
    ' block_file="$block_tmp" "$target" > "$tmp"
  else
    {
      cat "$target"
      printf '\n'
      agent_shim_block
    } > "$tmp"
  fi
  mv "$tmp" "$target"
  rm -f "$block_tmp"
}

validate_harness_markers() {
  local target="$1" label="$2"
  local begin_count end_count begin_line end_line
  begin_count=$(grep -Fc '<!-- HARNESS:BEGIN -->' "$target" || true)
  end_count=$(grep -Fc '<!-- HARNESS:END -->' "$target" || true)
  if [ "$begin_count" -eq 0 ] && [ "$end_count" -eq 0 ]; then
    return 0
  fi
  if [ "$begin_count" -ne 1 ] || [ "$end_count" -ne 1 ]; then
    fail "$label must contain exactly one complete Harness marker pair"
  fi
  begin_line=$(grep -Fn '<!-- HARNESS:BEGIN -->' "$target" | cut -d: -f1)
  end_line=$(grep -Fn '<!-- HARNESS:END -->' "$target" | cut -d: -f1)
  [ "$begin_line" -lt "$end_line" ] || fail "$label Harness markers are out of order"
}

refresh_agent_shim() {
  [ "$REFRESH_AGENT_SHIM" -eq 1 ] || return 0

  local target="$TARGET_DIR/AGENTS.md"
  [ -e "$target" ] || return 0

  if [ "$SOURCE_MODE" = "local" ] && [ "$SOURCE_ROOT/AGENTS.md" -ef "$target" ]; then
    log "skip     AGENTS.md (source file)"
    return 0
  fi

  validate_harness_markers "$target" "AGENTS.md"

  if [ "$DRY_RUN" -eq 1 ]; then
    if is_old_harness_agent_file "$target"; then
      log "refresh  AGENTS.md (old Harness guide -> shim, backup first)"
    else
      log "refresh  AGENTS.md (append or replace marked Harness block, backup first)"
    fi
    UPDATED=$((UPDATED + 1))
    return 0
  fi

  backup_agent_file
  if is_old_harness_agent_file "$target"; then
    local custom_tmp
    custom_tmp="$(mktemp)"
    extract_obvious_agent_custom_section "$target" "$custom_tmp"
    write_source_file "AGENTS.md" "$target"
    insert_agent_custom_section "$target" "$custom_tmp"
    rm -f "$custom_tmp"
    log "updated  AGENTS.md (old Harness guide -> shim; backup: ${BACKUP_DIR#$TARGET_DIR/}/AGENTS.md)"
  else
    append_or_replace_agent_harness_block
    log "updated  AGENTS.md (refreshed Harness block; backup: ${BACKUP_DIR#$TARGET_DIR/}/AGENTS.md)"
  fi
  UPDATED=$((UPDATED + 1))
}

backup_claude_file() {
  local target="$TARGET_DIR/CLAUDE.md"

  [ -e "$target" ] || return 0
  mkdir -p "$BACKUP_DIR"
  [ -e "$BACKUP_DIR/CLAUDE.md" ] && return 0
  cp -p "$target" "$BACKUP_DIR/CLAUDE.md"
}

write_claude_shim() {
  [ "$INSTALL_CLAUDE_SHIM" -eq 1 ] || return 0

  local target="$TARGET_DIR/CLAUDE.md"
  local block_tmp tmp

  if [ "$SOURCE_MODE" = "local" ] && [ -e "$target" ] &&
     [ "$SOURCE_ROOT/CLAUDE.md" -ef "$target" ]; then
    log "skip     CLAUDE.md (source file)"
    SKIPPED=$((SKIPPED + 1))
    return 0
  fi

  if [ -e "$target" ]; then
    validate_harness_markers "$target" "CLAUDE.md"
  fi

  block_tmp="$(mktemp)"
  claude_shim_block > "$block_tmp"

  if [ -e "$target" ] &&
     grep -Fq "<!-- HARNESS:BEGIN -->" "$target" &&
     grep -Fq "<!-- HARNESS:END -->" "$target"; then
    local current_tmp
    current_tmp="$(mktemp)"
    awk '
      /<!-- HARNESS:BEGIN -->/ { in_block = 1 }
      in_block { print }
      /<!-- HARNESS:END -->/ { in_block = 0 }
    ' "$target" > "$current_tmp"
    if cmp -s "$current_tmp" "$block_tmp"; then
      log "skip     CLAUDE.md (Harness block current)"
      SKIPPED=$((SKIPPED + 1))
      rm -f "$current_tmp" "$block_tmp"
      return 0
    fi
    rm -f "$current_tmp"

    if [ "$DRY_RUN" -eq 1 ]; then
      log "update   CLAUDE.md (refresh marked Harness block, backup first)"
    else
      backup_claude_file
      tmp="$(mktemp)"
      awk '
        /<!-- HARNESS:BEGIN -->/ {
          while ((getline line < block_file) > 0) {
            print line
          }
          in_block = 1
          next
        }
        /<!-- HARNESS:END -->/ && in_block {
          in_block = 0
          next
        }
        !in_block { print }
      ' block_file="$block_tmp" "$target" > "$tmp"
      mv "$tmp" "$target"
      log "updated  CLAUDE.md (refreshed Harness block; backup: ${BACKUP_DIR#$TARGET_DIR/}/CLAUDE.md)"
    fi
    UPDATED=$((UPDATED + 1))
  elif [ -e "$target" ]; then
    if [ "$DRY_RUN" -eq 1 ]; then
      log "update   CLAUDE.md (append Harness block, backup first)"
    else
      backup_claude_file
      {
        printf '\n'
        cat "$block_tmp"
      } >> "$target"
      log "updated  CLAUDE.md (appended Harness block; backup: ${BACKUP_DIR#$TARGET_DIR/}/CLAUDE.md)"
    fi
    UPDATED=$((UPDATED + 1))
  else
    if [ "$DRY_RUN" -eq 1 ]; then
      log "create   CLAUDE.md"
    else
      {
        printf '# Project Rules\n\n'
        cat "$block_tmp"
      } > "$target"
      log "created  CLAUDE.md"
    fi
    CREATED=$((CREATED + 1))
  fi
  rm -f "$block_tmp"
}

detect_cli_platform() {
  local os arch
  os="$(uname -s)"
  arch="$(uname -m)"

  case "$os:$arch" in
    Darwin:arm64)  printf 'macos-arm64' ;;
    Darwin:x86_64) printf 'macos-x64' ;;
    Linux:x86_64)  printf 'linux-x64' ;;
    Linux:aarch64|Linux:arm64) printf 'linux-arm64' ;;
    *)
      fail "Unsupported Harness CLI platform: $os/$arch."
      ;;
  esac
}

sha256_file() {
  local file="$1"
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$file" | awk '{ print $1 }'
  elif command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$file" | awk '{ print $1 }'
  else
    fail "shasum or sha256sum is required to verify the Harness CLI download"
  fi
}

download_file() {
  local url="$1"
  local target="$2"
  curl -fsSL "$url" -o "$target" || fail "Could not download $url"
}

read_cli_release_tag() {
  local tag_file="scripts/harness-cli-release-tag"
  local tag=""

  if [ "$SOURCE_MODE" = "local" ]; then
    if [ -f "$SOURCE_ROOT/$tag_file" ]; then
      tag="$(awk 'NF && $1 !~ /^#/ { print $1; exit }' "$SOURCE_ROOT/$tag_file")"
    fi
  else
    local tmp_file
    tmp_file="$(mktemp)"
    if ! curl -fsSL "$SOURCE_BASE_URL/$tag_file" -o "$tmp_file" 2>/dev/null; then
      echo "warning: could not fetch pinned CLI release tag from $SOURCE_BASE_URL/$tag_file; falling back to latest published release." >&2
    elif [ ! -s "$tmp_file" ]; then
      echo "warning: pinned CLI release tag at $SOURCE_BASE_URL/$tag_file was empty; falling back to latest published release." >&2
    else
      tag="$(awk 'NF && $1 !~ /^#/ { print $1; exit }' "$tmp_file")"
    fi
    rm -f "$tmp_file"
  fi

  printf '%s\n' "$tag"
}

default_cli_base_url() {
  local release_tag="${HARNESS_CLI_RELEASE_TAG:-}"

  if [ -z "$release_tag" ]; then
    release_tag="$(read_cli_release_tag)"
  fi

  if [ -n "$release_tag" ] && [ "$release_tag" != "latest" ]; then
    printf 'https://github.com/hoangnb24/repository-harness/releases/download/%s\n' "$release_tag"
  else
    printf 'https://github.com/hoangnb24/repository-harness/releases/latest/download\n'
  fi
}

read_harness_release_tag() {
  local tag_file="scripts/harness-release-tag"
  local tag=""
  if [ -n "${HARNESS_CORE_RELEASE_TAG:-}" ]; then
    printf '%s\n' "$HARNESS_CORE_RELEASE_TAG"
    return
  fi
  if [ "$SOURCE_MODE" = "local" ]; then
    [ -f "$SOURCE_ROOT/$tag_file" ] &&
      tag="$(awk 'NF && $1 !~ /^#/ { print $1; exit }' "$SOURCE_ROOT/$tag_file")"
  else
    local tag_tmp
    tag_tmp="$(mktemp)"
    if curl -fsSL "$CORE_SOURCE_BASE_URL/$tag_file" -o "$tag_tmp" 2>/dev/null; then
      tag="$(awk 'NF && $1 !~ /^#/ { print $1; exit }' "$tag_tmp")"
    fi
    rm -f "$tag_tmp"
  fi
  [ -n "$tag" ] || fail "Harness core release tag is missing"
  printf '%s\n' "$tag"
}

merge_core_gitignore() {
  local target="$1"
  local marker="# Harness core maintenance binary"
  local unix_rule="scripts/bin/harness"
  local windows_rule="scripts/bin/harness.exe"
  if [ -f "$target" ] && grep -Fxq "$unix_rule" "$target" && grep -Fxq "$windows_rule" "$target"; then
    log "skip     .gitignore (Harness core binary rules already present)"
    return
  fi
  if [ "$DRY_RUN" -eq 1 ]; then
    log "update   .gitignore (append Harness core binary rules)"
    return
  fi
  local missing_rules=()
  [ -f "$target" ] && grep -Fxq "$unix_rule" "$target" || missing_rules+=("$unix_rule")
  [ -f "$target" ] && grep -Fxq "$windows_rule" "$target" || missing_rules+=("$windows_rule")
  {
    [ -s "$target" ] && printf '\n'
    printf '%s\n' "$marker"
    printf '%s\n' "${missing_rules[@]}"
  } >> "$target"
  log "updated  .gitignore (appended Harness core binary rules)"
}

stage_harness_core_cli() {
  CORE_STAGE_ROOT="$(mktemp -d)"
  CORE_STAGED_BINARY="$CORE_STAGE_ROOT/harness"
  CORE_PLATFORM="${HARNESS_CORE_CLI_PLATFORM:-$(detect_cli_platform)}"
  CORE_BINARY_NAME="harness-$CORE_PLATFORM"
  if [ -n "${HARNESS_CORE_BINARY:-}" ]; then
    [ -x "$HARNESS_CORE_BINARY" ] || fail "HARNESS_CORE_BINARY is not executable: $HARNESS_CORE_BINARY"
    cp "$HARNESS_CORE_BINARY" "$CORE_STAGED_BINARY"
  elif [ "$SOURCE_MODE" = "local" ]; then
    command -v cargo >/dev/null 2>&1 || fail "cargo is required for a local Harness source install"
    cargo build --quiet --manifest-path "$SOURCE_ROOT/Cargo.toml" -p harness --locked
    cp "$SOURCE_ROOT/target/debug/harness" "$CORE_STAGED_BINARY"
  else
    local release_tag base_url binary_url checksum_url checksum_tmp expected actual
    if [ -n "${CORE_PENDING_VERSION:-}" ]; then
      release_tag="harness-v$CORE_PENDING_VERSION"
    else
      release_tag="$(read_harness_release_tag)"
    fi
    [[ "$release_tag" =~ ^harness-v[0-9]+\.[0-9]+\.[0-9]+([.-][A-Za-z0-9]+)*$ ]] ||
      fail "invalid Harness core release tag: $release_tag"
    base_url="${HARNESS_CORE_CLI_BASE_URL:-https://github.com/hoangnb24/repository-harness/releases/download/$release_tag}"
    binary_url="${base_url%/}/$CORE_BINARY_NAME"
    checksum_url="$binary_url.sha256"
    checksum_tmp="$CORE_STAGE_ROOT/$CORE_BINARY_NAME.sha256"
    download_file "$binary_url" "$CORE_STAGED_BINARY"
    download_file "$checksum_url" "$checksum_tmp"
    expected="$(awk '{ print $1; exit }' "$checksum_tmp")"
    actual="$(sha256_file "$CORE_STAGED_BINARY")"
    [ -n "$expected" ] && [ "$expected" = "$actual" ] ||
      fail "Checksum mismatch for $CORE_BINARY_NAME: expected $expected, got $actual"
    local reported_version
    chmod 755 "$CORE_STAGED_BINARY"
    reported_version="$("$CORE_STAGED_BINARY" --version | awk '{ print $NF; exit }')"
    [ "$reported_version" = "${release_tag#harness-v}" ] ||
      fail "Harness core release identity mismatch: tag=${release_tag#harness-v}, binary=$reported_version"
  fi
  chmod 755 "$CORE_STAGED_BINARY"
}

install_harness_core() {
  local command="install"
  [ -f "$TARGET_DIR/.harness-core/manifest.json" ] && command="update"
  CORE_PENDING_VERSION=""
  if [ "$command" = "update" ] && [ -f "$TARGET_DIR/.harness-core/update/session.json" ]; then
    CORE_PENDING_VERSION="$(sed -n 's/.*"to_version":[[:space:]]*"\([^"]*\)".*/\1/p' "$TARGET_DIR/.harness-core/update/session.json" | head -n 1)"
    [ -n "$CORE_PENDING_VERSION" ] || fail "could not read pending Harness update version"
  fi
  stage_harness_core_cli
  local args=("$command" --directory "$TARGET_DIR")
  [ "$command" = "update" ] && args+=(--candidate)
  [ -n "$CORE_PENDING_VERSION" ] && args+=(--continue)
  [ "$DRY_RUN" -eq 1 ] && args+=(--dry-run)
  local runner="$CORE_STAGED_BINARY"
  local binary_target="" binary_temp=""
  if [ "$DRY_RUN" -eq 0 ]; then
    binary_target="$TARGET_DIR/scripts/bin/harness"
    binary_temp="$TARGET_DIR/scripts/bin/.harness.$$.tmp"
    [ ! -L "$TARGET_DIR/scripts" ] || fail "refusing symlink for repository scripts directory"
    [ ! -L "$TARGET_DIR/scripts/bin" ] || fail "refusing symlink for repository scripts/bin directory"
    [ ! -L "$binary_target" ] || fail "refusing symlink for repository Harness executable"
    mkdir -p "$(dirname "$binary_target")"
    cp "$CORE_STAGED_BINARY" "$binary_temp"
    chmod 755 "$binary_temp"
    if [ -e "$binary_target" ]; then
      mkdir -p "$BACKUP_DIR/scripts/bin"
      cp -p "$binary_target" "$BACKUP_DIR/scripts/bin/harness"
    fi
  fi
  set +e
  "$runner" "${args[@]}"
  local command_status=$?
  set -e
  if [ "$command_status" -eq 2 ] && [ "$DRY_RUN" -eq 0 ]; then
    local retained="$TARGET_DIR/.harness-core/update-candidate/harness"
    [ ! -L "$TARGET_DIR/.harness-core" ] || fail "refusing symlink for .harness-core"
    [ ! -L "$TARGET_DIR/.harness-core/update-candidate" ] || fail "refusing symlink for retained candidate directory"
    [ ! -L "$retained" ] || fail "refusing symlink for retained update candidate"
    mkdir -p "$(dirname "$retained")"
    cp "$CORE_STAGED_BINARY" "$retained"
    chmod 755 "$retained"
  fi
  if [ "$command_status" -eq 0 ] && [ "$DRY_RUN" -eq 0 ]; then
    mv -f "$binary_temp" "$binary_target"
    rm -rf "$TARGET_DIR/.harness-core/update-candidate"
    merge_core_gitignore "$TARGET_DIR/.gitignore"
    log "installed scripts/bin/harness ($CORE_PLATFORM)"
  elif [ -n "$binary_temp" ]; then
    rm -f "$binary_temp"
  fi
  rm -rf "$CORE_STAGE_ROOT"
  CORE_STAGE_ROOT=""
  if [ "$command_status" -eq 2 ]; then
    fail "Harness core update needs resolution; edit .harness-core/update/resolved/, then rerun this installer or harness update --continue"
  fi
  [ "$command_status" -eq 0 ] || fail "harness $command failed with exit code $command_status"
}

prepare_cli_identity() {
  CLI_PLATFORM="${HARNESS_CLI_PLATFORM:-$(detect_cli_platform)}"
  CLI_BINARY_NAME="harness-cli-$CLI_PLATFORM"
  CLI_TARGET_RELATIVE="scripts/bin/harness-cli"
}

cli_binary_is_preserved() {
  [ -e "$TARGET_DIR/$CLI_TARGET_RELATIVE" ] &&
    [ "$CONFLICT_ACTION" = "merge" ] &&
    [ "$FORCE" -eq 0 ] &&
    [ "$UPGRADE_CLI" -eq 0 ]
}

plan_harness_cli_binary() {
  if cli_binary_is_preserved; then
    log "skip     scripts/bin/harness-cli (merge keeps existing file)"
    SKIPPED=$((SKIPPED + 1))
    return 0
  fi

  log "download $CLI_BINARY_NAME -> scripts/bin/harness-cli"
  log "verify   $CLI_BINARY_NAME.sha256"
  if [ -e "$TARGET_DIR/$CLI_TARGET_RELATIVE" ]; then
    UPDATED=$((UPDATED + 1))
  else
    CREATED=$((CREATED + 1))
  fi
}

stage_harness_cli_binary() {
  local stage_root="$1"
  local binary_url checksum_url binary_tmp checksum_tmp expected actual

  cli_binary_is_preserved && return 0

  command -v curl >/dev/null 2>&1 || fail "curl is required to download the Harness CLI"

  binary_url="$CLI_BASE_URL/$CLI_BINARY_NAME"
  checksum_url="$binary_url.sha256"
  binary_tmp="$stage_root/.binary/$CLI_BINARY_NAME"
  checksum_tmp="$binary_tmp.sha256"
  mkdir -p "$(dirname "$binary_tmp")"

  download_file "$binary_url" "$binary_tmp"
  download_file "$checksum_url" "$checksum_tmp"

  expected="$(awk '{ print $1; exit }' "$checksum_tmp")"
  [ -n "$expected" ] || fail "Checksum file is empty: $checksum_url"
  actual="$(sha256_file "$binary_tmp")"
  if [ "$actual" != "$expected" ]; then
    fail "Checksum mismatch for $CLI_BINARY_NAME: expected $expected, got $actual"
  fi
  chmod 755 "$binary_tmp"
}

apply_staged_harness_cli_binary() {
  local stage_root="$1"
  local target="$TARGET_DIR/$CLI_TARGET_RELATIVE"
  local binary_tmp="$stage_root/.binary/$CLI_BINARY_NAME"

  if cli_binary_is_preserved; then
    log "skip     scripts/bin/harness-cli (merge keeps existing file)"
    SKIPPED=$((SKIPPED + 1))
    return 0
  fi

  if [ -e "$target" ]; then
    if [ "$FORCE" -eq 1 ] || [ "$UPGRADE_CLI" -eq 1 ]; then
      mkdir -p "$BACKUP_DIR/scripts/bin"
      cp -p "$target" "$BACKUP_DIR/scripts/bin/harness-cli"
    fi
    UPDATED=$((UPDATED + 1))
    log "updated  scripts/bin/harness-cli"
  else
    CREATED=$((CREATED + 1))
    log "created  scripts/bin/harness-cli"
  fi

  mkdir -p "$(dirname "$target")"
  mv -f "$binary_tmp" "$target"
  log "verified scripts/bin/harness-cli ($CLI_PLATFORM)"
}

read_cli_bundle_files() {
  local manifest relative schema_count=0
  manifest="$(read_payload_manifest "$CLI_PAYLOAD_MANIFEST")"
  while IFS= read -r relative || [ -n "$relative" ]; do
    relative="${relative%$'\r'}"
    case "$relative" in
      ""|\#*) continue ;;
    esac
    printf '%s\n' "$relative"
  done <<EOF
$manifest
EOF

  while IFS= read -r relative || [ -n "$relative" ]; do
    [ -n "$relative" ] || continue
    printf '%s\n' "$relative"
    schema_count=$((schema_count + 1))
  done <<EOF
$(discover_schema_files)
EOF
  [ "$schema_count" -gt 0 ] || fail "No schema migrations found in $SCHEMA_DIR"
}

snapshot_cli_bundle_targets() {
  local relative target snapshot
  CLI_ROLLBACK_STATE="$CLI_STAGE_ROOT/.rollback-state"
  CLI_ROLLBACK_ROOT="$CLI_STAGE_ROOT/.rollback"
  : > "$CLI_ROLLBACK_STATE"
  while IFS= read -r relative || [ -n "$relative" ]; do
    [ -n "$relative" ] || continue
    target="$TARGET_DIR/$relative"
    if [ -e "$target" ]; then
      snapshot="$CLI_ROLLBACK_ROOT/$relative"
      mkdir -p "$(dirname "$snapshot")"
      cp -p "$target" "$snapshot"
      printf 'existing\t%s\n' "$relative" >> "$CLI_ROLLBACK_STATE"
    else
      printf 'absent\t%s\n' "$relative" >> "$CLI_ROLLBACK_STATE"
    fi
  done <<EOF
$CLI_BUNDLE_FILES
.gitignore
$CLI_TARGET_RELATIVE
EOF
}

rollback_cli_bundle() {
  local state relative target snapshot
  [ -f "${CLI_ROLLBACK_STATE:-}" ] || return 0
  while IFS=$'\t' read -r state relative || [ -n "${state:-}" ]; do
    [ -n "${relative:-}" ] || continue
    target="$TARGET_DIR/$relative"
    if [ "$state" = "existing" ]; then
      snapshot="$CLI_ROLLBACK_ROOT/$relative"
      mkdir -p "$(dirname "$target")"
      cp -p "$snapshot" "$target"
    else
      rm -f "$target"
    fi
  done < "$CLI_ROLLBACK_STATE"
  printf 'Warning: optional CLI bundle failed; restored its previous files.\n' >&2
}

cleanup_cli_bundle_on_exit() {
  local exit_code=$?
  trap - EXIT
  if [ "${CLI_ROLLBACK_ARMED:-0}" -eq 1 ]; then
    rollback_cli_bundle
  fi
  if [ -n "${CLI_STAGE_ROOT:-}" ] && [ -d "$CLI_STAGE_ROOT" ]; then
    rm -rf "$CLI_STAGE_ROOT"
  fi
  exit "$exit_code"
}

install_cli_bundle() {
  [ "$INSTALL_RUST_CLI" -eq 1 ] || return 0

  local relative staged_target previous_source_mode previous_source_root
  prepare_cli_identity
  CLI_BUNDLE_FILES="$(read_cli_bundle_files)"

  if [ "$DRY_RUN" -eq 1 ]; then
    while IFS= read -r relative || [ -n "$relative" ]; do
      [ -n "$relative" ] || continue
      copy_file "$relative"
    done <<EOF
$CLI_BUNDLE_FILES
EOF
    merge_gitignore "$TARGET_DIR/.gitignore"
    plan_harness_cli_binary
    return 0
  fi

  CLI_STAGE_ROOT="$(mktemp -d)"
  CLI_ROLLBACK_ARMED=0
  trap cleanup_cli_bundle_on_exit EXIT
  while IFS= read -r relative || [ -n "$relative" ]; do
    [ -n "$relative" ] || continue
    staged_target="$CLI_STAGE_ROOT/$relative"
    mkdir -p "$(dirname "$staged_target")"
    write_source_file "$relative" "$staged_target"
  done <<EOF
$CLI_BUNDLE_FILES
EOF
  stage_harness_cli_binary "$CLI_STAGE_ROOT"
  snapshot_cli_bundle_targets

  CLI_ROLLBACK_ARMED=1
  previous_source_mode="$SOURCE_MODE"
  previous_source_root="$SOURCE_ROOT"
  SOURCE_MODE="local"
  SOURCE_ROOT="$CLI_STAGE_ROOT"
  while IFS= read -r relative || [ -n "$relative" ]; do
    [ -n "$relative" ] || continue
    copy_file "$relative"
  done <<EOF
$CLI_BUNDLE_FILES
EOF
  SOURCE_MODE="$previous_source_mode"
  SOURCE_ROOT="$previous_source_root"

  merge_gitignore "$TARGET_DIR/.gitignore"
  apply_staged_harness_cli_binary "$CLI_STAGE_ROOT"
  if [ -f "$TARGET_DIR/scripts/bootstrap-harness.sh" ]; then
    chmod 755 "$TARGET_DIR/scripts/bootstrap-harness.sh"
  fi

  CLI_ROLLBACK_ARMED=0
  trap - EXIT
  rm -rf "$CLI_STAGE_ROOT"
  CLI_STAGE_ROOT=""
}

check_protected_target_paths() {
  local conflicts=()

  [ -e "$TARGET_DIR/AGENTS.md" ] && conflicts+=("AGENTS.md")
  [ -e "$TARGET_DIR/docs" ] && conflicts+=("docs/")
  if [ "$INSTALL_RUST_CLI" -eq 1 ] && [ -e "$TARGET_DIR/scripts" ]; then
    conflicts+=("scripts/")
  fi

  [ "${#conflicts[@]}" -gt 0 ] || return 0

  local joined=""
  local item
  for item in "${conflicts[@]}"; do
    if [ -n "$joined" ]; then
      joined="$joined, $item"
    else
      joined="$item"
    fi
  done

  case "$REQUESTED_CONFLICT_ACTION" in
    merge)
      CONFLICT_ACTION="merge"
      log "Continuing with merge. Existing files will be skipped."
      return 0
      ;;
    override)
      CONFLICT_ACTION="override"
      override_protected_target_paths
      return 0
      ;;
    stop)
      warn_stop "target already contains protected Harness paths: $joined. Refusing to install so existing project instructions or docs are not mixed or overwritten."
      ;;
  esac

  if [ "$YES" -eq 1 ] || ! can_prompt; then
    warn_stop "target already contains protected Harness paths: $joined. Refusing to install so existing project instructions or docs are not mixed or overwritten. Use an empty target directory, or move those paths before running the installer."
  fi

  {
    printf 'Warning: target already contains protected Harness paths: %s\n' "$joined"
    printf 'Choose how to continue:\n'
    printf '  1. Merge    Copy missing Harness files and skip existing files\n'
    printf '  2. Override Back up and replace AGENTS.md, docs/, and scripts/\n'
    printf '  3. Stop     Exit without writing files (recommended)\n'
  } > /dev/tty
  prompt_tty 'Choice [1/2/3, default 3]: '

  local choice
  choice="$(read_tty)"
  case "$choice" in
    1|m|M|merge|Merge)
      CONFLICT_ACTION="merge"
      log "Continuing with merge. Existing files will be skipped."
      ;;
    2|o|O|override|Override)
      CONFLICT_ACTION="override"
      override_protected_target_paths
      ;;
    ""|3|s|S|stop|Stop)
      warn_stop "installation stopped by user."
      ;;
    *)
      warn_stop "unknown choice: $choice"
      ;;
  esac
}

override_protected_target_paths() {
  local protected

  for protected in AGENTS.md docs; do
    [ -e "$TARGET_DIR/$protected" ] || continue

    if [ "$DRY_RUN" -eq 1 ]; then
      log "override $protected (backup first)"
      continue
    fi

    mkdir -p "$BACKUP_DIR"
    mv "$TARGET_DIR/$protected" "$BACKUP_DIR/$protected"
    log "removed  $protected (backup: ${BACKUP_DIR#$TARGET_DIR/}/$protected)"
  done

  if [ "$INSTALL_RUST_CLI" -eq 1 ] && [ -e "$TARGET_DIR/scripts" ]; then
    if [ "$DRY_RUN" -eq 1 ]; then
      log "override scripts (backup first)"
    else
      mkdir -p "$BACKUP_DIR"
      mv "$TARGET_DIR/scripts" "$BACKUP_DIR/scripts"
      log "removed  scripts (backup: ${BACKUP_DIR#$TARGET_DIR/}/scripts)"
    fi
  fi
}

install_engineering_wisdom() {
  [ "$INSTALL_ENGINEERING_WISDOM" -eq 1 ] || return 0
  copy_manifest_files "$ENGINEERING_WISDOM_PAYLOAD_MANIFEST"
}

TARGET_INPUT="${HARNESS_TARGET_DIR:-$PWD}"
YES=0
FORCE=0
DRY_RUN=0
INSTALL_RUST_CLI=0
INSTALL_ENGINEERING_WISDOM=0
REFRESH_AGENT_SHIM=0
INSTALL_CLAUDE_SHIM=0
UPGRADE_CLI=0
REQUESTED_REF=""
REQUESTED_CONFLICT_ACTION=""
POSITIONAL_TARGET=""

while [ "$#" -gt 0 ]; do
  case "$1" in
    -d|--directory)
      [ "$#" -ge 2 ] || fail "$1 requires a path"
      TARGET_INPUT="$2"
      shift 2
      ;;
    -y|--yes)
      YES=1
      shift
      ;;
    --with-cli)
      INSTALL_RUST_CLI=1
      shift
      ;;
    --with-engineering-wisdom)
      INSTALL_ENGINEERING_WISDOM=1
      shift
      ;;
    --force)
      FORCE=1
      shift
      ;;
    --merge)
      REQUESTED_CONFLICT_ACTION="merge"
      shift
      ;;
    --upgrade-cli)
      UPGRADE_CLI=1
      shift
      ;;
    --ref)
      [ "$#" -ge 2 ] || fail "$1 requires an immutable Harness release tag"
      REQUESTED_REF="$2"
      shift 2
      ;;
    --refresh-agent-shim)
      REFRESH_AGENT_SHIM=1
      shift
      ;;
    --claude)
      INSTALL_CLAUDE_SHIM=1
      shift
      ;;
    --override)
      REQUESTED_CONFLICT_ACTION="override"
      shift
      ;;
    --stop)
      REQUESTED_CONFLICT_ACTION="stop"
      shift
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      break
      ;;
    -*)
      fail "Unknown option: $1"
      ;;
    *)
      [ -z "$POSITIONAL_TARGET" ] || fail "Only one target path is supported"
      POSITIONAL_TARGET="$1"
      shift
      ;;
  esac
done

if [ "$#" -gt 0 ]; then
  [ -z "$POSITIONAL_TARGET" ] || fail "Only one target path is supported"
  POSITIONAL_TARGET="$1"
  shift
fi

[ "$#" -eq 0 ] || fail "Unexpected extra arguments"

if [ -n "$POSITIONAL_TARGET" ]; then
  TARGET_INPUT="$POSITIONAL_TARGET"
fi

SCRIPT_PATH="${BASH_SOURCE[0]:-$0}"
SCRIPT_DIR="$(cd "$(dirname "$SCRIPT_PATH")" 2>/dev/null && pwd -P || printf '')"
SOURCE_ROOT=""
SOURCE_MODE="remote"
SOURCE_BASE_URL="${HARNESS_SOURCE_BASE_URL:-https://raw.githubusercontent.com/hoangnb24/repository-harness/main}"
SOURCE_BASE_URL="${SOURCE_BASE_URL%/}"
CORE_SOURCE_BASE_URL="${HARNESS_CORE_SOURCE_BASE_URL:-https://raw.githubusercontent.com/hoangnb24/repository-harness/main}"
CORE_SOURCE_BASE_URL="${CORE_SOURCE_BASE_URL%/}"
PAYLOAD_MANIFEST="scripts/harness-install-files.txt"
CLI_PAYLOAD_MANIFEST="scripts/harness-cli-install-files.txt"
ENGINEERING_WISDOM_PAYLOAD_MANIFEST="scripts/engineering-wisdom-install-files.txt"
SCHEMA_DIR="scripts/schema"
CLI_BASE_URL="${HARNESS_CLI_BASE_URL:-}"
CLI_BASE_URL="${CLI_BASE_URL%/}"

if [ "$UPGRADE_CLI" -eq 0 ] && [ -n "$REQUESTED_REF" ]; then
  fail "--ref is valid only with --upgrade-cli"
fi

if [ "$UPGRADE_CLI" -eq 1 ]; then
  INSTALL_RUST_CLI=1
  [ -n "$REQUESTED_REF" ] || fail "--upgrade-cli requires --ref <harness-cli-vX.Y.Z>"
  [[ "$REQUESTED_REF" =~ ^harness-cli-v[0-9]+\.[0-9]+\.[0-9]+([.-][A-Za-z0-9]+)*$ ]] ||
    fail "--ref must be an immutable Harness CLI release tag such as harness-cli-v0.1.14"
  SOURCE_MODE="remote"
  SOURCE_ROOT=""
  SOURCE_BASE_URL="${HARNESS_SOURCE_BASE_URL:-https://raw.githubusercontent.com/hoangnb24/repository-harness/$REQUESTED_REF}"
  SOURCE_BASE_URL="${SOURCE_BASE_URL%/}"
  CLI_BASE_URL="${HARNESS_CLI_BASE_URL:-https://github.com/hoangnb24/repository-harness/releases/download/$REQUESTED_REF}"
  CLI_BASE_URL="${CLI_BASE_URL%/}"
  REFRESH_AGENT_SHIM=1
fi

if [ "$UPGRADE_CLI" -eq 0 ] && [ -n "$SCRIPT_DIR" ] && [ -f "$SCRIPT_DIR/../AGENTS.md" ] && [ -f "$SCRIPT_DIR/../docs/HARNESS.md" ]; then
  SOURCE_ROOT="$(cd "$SCRIPT_DIR/.." && pwd -P)"
  SOURCE_MODE="local"
fi

if [ "$INSTALL_RUST_CLI" -eq 1 ] && [ -z "$CLI_BASE_URL" ]; then
  CLI_BASE_URL="$(default_cli_base_url)"
fi

if [ "$YES" -eq 0 ] && can_prompt; then
  prompt_tty "Install Harness v0 into [$TARGET_INPUT]: "
  REPLY_TARGET="$(read_tty)"
  if [ -n "$REPLY_TARGET" ]; then
    TARGET_INPUT="$REPLY_TARGET"
  fi
fi

TARGET_DIR="$(make_absolute_parent "$(expand_path "$TARGET_INPUT")")"
BACKUP_DIR="$TARGET_DIR/.harness-backup/$(date +%Y%m%d%H%M%S)"
CREATED=0
UPDATED=0
SKIPPED=0
CONFLICT_ACTION="install"

if [ "$DRY_RUN" -eq 1 ]; then
  log "Dry run: no files will be written."
elif [ ! -d "$TARGET_DIR" ]; then
  mkdir -p "$TARGET_DIR"
fi

if [ ! -d "$TARGET_DIR" ]; then
  [ "$DRY_RUN" -eq 1 ] || fail "Target directory could not be created: $TARGET_DIR"
  log "Target directory would be created: $TARGET_DIR"
fi

if [ -d "$TARGET_DIR" ]; then
  [ -w "$TARGET_DIR" ] || fail "Target directory is not writable: $TARGET_DIR"
else
  [ -w "$(dirname "$TARGET_DIR")" ] || fail "Target parent directory is not writable: $(dirname "$TARGET_DIR")"
fi

if [ -d "$TARGET_DIR" ]; then
  check_protected_target_paths
fi

if [ "$SOURCE_MODE" = "local" ]; then
  log "Harness source: $SOURCE_ROOT"
else
  command -v curl >/dev/null 2>&1 || fail "curl is required for remote installation"
  log "Harness source: $SOURCE_BASE_URL"
fi
if [ "$INSTALL_RUST_CLI" -eq 1 ]; then
  log "Harness profile: core+cli"
else
  log "Harness profile: core"
fi
if [ "$INSTALL_RUST_CLI" -eq 1 ]; then
  log "Harness CLI source: $CLI_BASE_URL"
else
  log "Harness CLI source: skipped"
fi
if [ "$INSTALL_ENGINEERING_WISDOM" -eq 1 ]; then
  log "Engineering wisdom: included (explicit opt-in)"
else
  log "Engineering wisdom: excluded"
fi
log "Target project: $TARGET_DIR"

install_harness_core

install_engineering_wisdom
refresh_agent_shim
write_claude_shim
install_cli_bundle

log ""
log "Done. Created: $CREATED, updated: $UPDATED, skipped: $SKIPPED."

if [ "$SKIPPED" -gt 0 ] && [ "$FORCE" -eq 0 ]; then
  log "Existing files were left untouched. Re-run with --force to overwrite with backups."
fi

if [ "$FORCE" -eq 1 ] && [ "$UPDATED" -gt 0 ] && [ "$DRY_RUN" -eq 0 ]; then
  log "Backups were written to: $BACKUP_DIR"
fi
