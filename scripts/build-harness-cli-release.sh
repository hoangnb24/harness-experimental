#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: build-harness-cli-release.sh [options]

Build a prebuilt Harness Rust CLI artifact and checksum.

Options:
      --target <triple>  Cargo target triple. Defaults to the host target.
      --profile <name>   Cargo profile. Defaults to release.
      --out-dir <path>   Artifact directory. Defaults to dist.
  -h, --help             Show this help.

The produced files are:

  dist/harness-cli-<platform>
  dist/harness-cli-<platform>.sha256
  dist/harness-cli-windows-x64.exe
  dist/harness-cli-windows-x64.exe.sha256

Supported platform labels:

  aarch64-apple-darwin      -> macos-arm64
  x86_64-apple-darwin       -> macos-x64
  x86_64-unknown-linux-gnu  -> linux-x64
  aarch64-unknown-linux-gnu -> linux-arm64
  x86_64-pc-windows-msvc    -> windows-x64
EOF
}

fail() {
  printf 'Error: %s\n' "$*" >&2
  exit 1
}

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
target=""
profile="release"
out_dir="$repo_root/dist"

while [ "$#" -gt 0 ]; do
  case "$1" in
    --target)
      [ "$#" -ge 2 ] || fail "$1 requires a target triple"
      target="$2"
      shift 2
      ;;
    --profile)
      [ "$#" -ge 2 ] || fail "$1 requires a profile"
      profile="$2"
      shift 2
      ;;
    --out-dir)
      [ "$#" -ge 2 ] || fail "$1 requires a path"
      out_dir="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      fail "Unknown option: $1"
      ;;
  esac
done

if [ -n "$target" ]; then
  cargo_args=(build --package harness-cli --profile "$profile" --target "$target")
  triple="$target"
else
  cargo_args=(build --package harness-cli --profile "$profile")
  triple="$(rustc -vV | awk '/^host:/ { print $2 }')"
fi

case "$triple" in
  aarch64-apple-darwin) platform="macos-arm64" ;;
  x86_64-apple-darwin) platform="macos-x64" ;;
  x86_64-unknown-linux-gnu) platform="linux-x64" ;;
  aarch64-unknown-linux-gnu) platform="linux-arm64" ;;
  x86_64-pc-windows-msvc) platform="windows-x64" ;;
  *) fail "Unsupported release target: $triple" ;;
esac

binary_name="harness-cli"
artifact_name="harness-cli-$platform"
if [ "$platform" = "windows-x64" ]; then
  binary_name="harness-cli.exe"
  artifact_name="$artifact_name.exe"
fi

if [ -n "$target" ]; then
  binary="$repo_root/target/$target/$profile/$binary_name"
else
  binary="$repo_root/target/$profile/$binary_name"
fi

(
  cd "$repo_root"
  # Always build release artifacts with --locked so the binary only links the
  # exact dependency set that has been verified by repository CI. A release
  # artifact that resolves a never-reviewed dependency version defeats the
  # reproducibility promise the installer's SHA-256 check is meant to enforce.
  cargo "${cargo_args[@]}" --locked
)

[ -f "$binary" ] || fail "Expected compiled binary missing: $binary"

mkdir -p "$out_dir"
artifact="$out_dir/$artifact_name"
cp "$binary" "$artifact"
if [ "$platform" != "windows-x64" ]; then
  chmod 755 "$artifact"
fi

if command -v shasum >/dev/null 2>&1; then
  (cd "$out_dir" && shasum -a 256 "$(basename "$artifact")" > "$(basename "$artifact").sha256")
elif command -v sha256sum >/dev/null 2>&1; then
  (cd "$out_dir" && sha256sum "$(basename "$artifact")" > "$(basename "$artifact").sha256")
else
  fail "shasum or sha256sum is required to write checksums"
fi

printf 'Built %s\n' "$artifact"
printf 'Wrote %s.sha256\n' "$artifact"
