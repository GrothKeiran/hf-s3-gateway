#!/usr/bin/env bash
set -euo pipefail

: "${S3_ENDPOINT:?set S3_ENDPOINT, for example http://127.0.0.1:9000}"
: "${S3_BUCKET:?set S3_BUCKET}"
: "${S3_ACCESS_KEY:?set S3_ACCESS_KEY}"
: "${S3_SECRET_KEY:?set S3_SECRET_KEY}"

fail() { printf 'error: %s\n' "$*" >&2; exit 1; }
command -v s3fs >/dev/null 2>&1 || fail "s3fs is not installed"
if [[ ! -e /dev/fuse ]]; then
  fail "/dev/fuse is missing; run on a host/container with FUSE enabled"
fi
if ! command -v fusermount >/dev/null 2>&1 && ! command -v fusermount3 >/dev/null 2>&1; then
  fail "fusermount or fusermount3 is required"
fi

WORKDIR="${COMPAT_TMP:-$(mktemp -d)}"
MOUNTDIR="$WORKDIR/mnt"
PASSFILE="$WORKDIR/s3fs.passwd"
PREFIX="${COMPAT_PREFIX:-s3fs-smoke/$(date +%s)-$$}"
KEY="$PREFIX/file.txt"

cleanup() {
  fusermount -u "$MOUNTDIR" >/dev/null 2>&1 || fusermount3 -u "$MOUNTDIR" >/dev/null 2>&1 || true
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

mkdir -p "$MOUNTDIR"
printf '%s:%s\n' "$S3_ACCESS_KEY" "$S3_SECRET_KEY" > "$PASSFILE"
chmod 600 "$PASSFILE"

s3fs "$S3_BUCKET" "$MOUNTDIR" \
  -o passwd_file="$PASSFILE" \
  -o url="$S3_ENDPOINT" \
  -o use_path_request_style \
  -o dbglevel=warn

mkdir -p "$MOUNTDIR/$PREFIX"
printf 'hello via s3fs\n' > "$WORKDIR/source.txt"
cp "$WORKDIR/source.txt" "$MOUNTDIR/$KEY"
sync
cmp "$WORKDIR/source.txt" "$MOUNTDIR/$KEY"
rm -f "$MOUNTDIR/$KEY"

printf 's3fs smoke completed for %s/%s\n' "$S3_BUCKET" "$PREFIX"
