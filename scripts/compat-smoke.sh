#!/usr/bin/env bash
set -euo pipefail

: "${S3_ENDPOINT:?set S3_ENDPOINT, for example http://127.0.0.1:9000}"
: "${S3_BUCKET:?set S3_BUCKET}"
: "${S3_ACCESS_KEY:?set S3_ACCESS_KEY}"
: "${S3_SECRET_KEY:?set S3_SECRET_KEY}"

WORKDIR="${COMPAT_TMP:-$(mktemp -d)}"
PREFIX="${COMPAT_PREFIX:-compat-smoke/$(date +%s)-$$}"
SMALL_KEY="$PREFIX/small.txt"
BIG_KEY="$PREFIX/multipart.bin"
COPY_KEY="$PREFIX/copy.txt"
DELETE_A_KEY="$PREFIX/delete-a.txt"
DELETE_B_KEY="$PREFIX/delete-b.txt"

cleanup() {
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

log() {
  printf '\n==> %s\n' "$*"
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    printf 'skip: %s not installed\n' "$1" >&2
    return 1
  }
}

write_fixture_files() {
  printf 'hello from hf-s3-gateway compatibility smoke\n' > "$WORKDIR/small.txt"
  python3 - <<PY
from pathlib import Path
Path("$WORKDIR/multipart.bin").write_bytes((b"0123456789abcdef" * 1024 * 1024)[:10 * 1024 * 1024])
PY
}

object_url() {
  printf '%s/%s/%s' "${S3_ENDPOINT%/}" "$S3_BUCKET" "$1"
}

bucket_url() {
  printf '%s/%s%s' "${S3_ENDPOINT%/}" "$S3_BUCKET" "$1"
}

check_gateway_bucket() {
  require_cmd curl || return 0
  health_json="$(curl -fsS "${S3_ENDPOINT%/}/healthz" || true)"
  if [[ -z "$health_json" ]]; then
    return 0
  fi
  gateway_bucket="$(HEALTH_JSON="$health_json" python3 - <<'PY'
import json, os
try:
    print(json.loads(os.environ.get('HEALTH_JSON', '{}')).get('bucket', ''))
except Exception:
    print('')
PY
)"
  if [[ -n "$gateway_bucket" && "$gateway_bucket" != "$S3_BUCKET" ]]; then
    printf 'error: gateway exposes bucket %q but S3_BUCKET is %q. Set server HF_BUCKET and client S3_BUCKET to the same value.\n' "$gateway_bucket" "$S3_BUCKET" >&2
    exit 2
  fi
}

curl_smoke() {
  require_cmd curl || return 0
  log "curl/basic: upload, head, range, list, batch delete"
  curl -fsS -u "$S3_ACCESS_KEY:$S3_SECRET_KEY" -X PUT --data-binary "@$WORKDIR/small.txt" "$(object_url "$SMALL_KEY")" >/dev/null
  curl -fsSI -u "$S3_ACCESS_KEY:$S3_SECRET_KEY" "$(object_url "$SMALL_KEY")" >/dev/null
  curl -fsS -u "$S3_ACCESS_KEY:$S3_SECRET_KEY" -H 'Range: bytes=0-4' "$(object_url "$SMALL_KEY")" -o "$WORKDIR/curl-range.txt"
  test "$(cat "$WORKDIR/curl-range.txt")" = "hello"
  curl -fsS -u "$S3_ACCESS_KEY:$S3_SECRET_KEY" "$(bucket_url "?list-type=2&prefix=$PREFIX/")" | grep -F "$SMALL_KEY" >/dev/null
  curl -fsS -u "$S3_ACCESS_KEY:$S3_SECRET_KEY" -X PUT --data-binary "@$WORKDIR/small.txt" "$(object_url "$DELETE_A_KEY")" >/dev/null
  curl -fsS -u "$S3_ACCESS_KEY:$S3_SECRET_KEY" -X PUT --data-binary "@$WORKDIR/small.txt" "$(object_url "$DELETE_B_KEY")" >/dev/null
  curl -fsS -u "$S3_ACCESS_KEY:$S3_SECRET_KEY" -X POST -H 'Content-Type: application/xml' --data-binary "<Delete><Object><Key>$DELETE_A_KEY</Key></Object><Object><Key>$DELETE_B_KEY</Key></Object></Delete>" "$(bucket_url '?delete')" >/dev/null
  curl -fsS -u "$S3_ACCESS_KEY:$S3_SECRET_KEY" -X DELETE "$(object_url "$SMALL_KEY")" >/dev/null
}

aws_cli_smoke() {
  require_cmd aws || return 0
  require_cmd curl || return 0
  log "aws cli: bucket, upload, head, range, presign, multipart, copy, batch delete"
  export AWS_ACCESS_KEY_ID="$S3_ACCESS_KEY"
  export AWS_SECRET_ACCESS_KEY="$S3_SECRET_KEY"
  export AWS_EC2_METADATA_DISABLED=true
  export AWS_DEFAULT_REGION="${S3_REGION:-us-east-1}"
  export AWS_CONFIG_FILE="$WORKDIR/aws-config"
  cat > "$AWS_CONFIG_FILE" <<EOF
[default]
region = $AWS_DEFAULT_REGION
s3 =
    signature_version = s3v4
EOF

  aws --endpoint-url "$S3_ENDPOINT" s3api head-bucket --bucket "$S3_BUCKET"
  aws --endpoint-url "$S3_ENDPOINT" s3api list-objects-v2 --bucket "$S3_BUCKET" --prefix "$PREFIX/" --max-keys 1 >/dev/null
  aws --endpoint-url "$S3_ENDPOINT" s3api put-object --bucket "$S3_BUCKET" --key "$SMALL_KEY" --body "$WORKDIR/small.txt" >/dev/null
  aws --endpoint-url "$S3_ENDPOINT" s3api head-object --bucket "$S3_BUCKET" --key "$SMALL_KEY" >/dev/null
  aws --endpoint-url "$S3_ENDPOINT" s3api get-object --bucket "$S3_BUCKET" --key "$SMALL_KEY" --range bytes=0-4 "$WORKDIR/range.txt" >/dev/null
  test "$(cat "$WORKDIR/range.txt")" = "hello"

  presigned="$(aws --endpoint-url "$S3_ENDPOINT" s3 presign "s3://$S3_BUCKET/$SMALL_KEY" --expires-in 120)"
  curl -fsSL "$presigned" -o "$WORKDIR/presigned.txt"
  cmp "$WORKDIR/small.txt" "$WORKDIR/presigned.txt"

  aws --endpoint-url "$S3_ENDPOINT" s3 cp "$WORKDIR/multipart.bin" "s3://$S3_BUCKET/$BIG_KEY" >/dev/null
  aws --endpoint-url "$S3_ENDPOINT" s3api copy-object --bucket "$S3_BUCKET" --key "$COPY_KEY" --copy-source "/$S3_BUCKET/$SMALL_KEY" >/dev/null
  aws --endpoint-url "$S3_ENDPOINT" s3api put-object --bucket "$S3_BUCKET" --key "$DELETE_A_KEY" --body "$WORKDIR/small.txt" >/dev/null
  aws --endpoint-url "$S3_ENDPOINT" s3api put-object --bucket "$S3_BUCKET" --key "$DELETE_B_KEY" --body "$WORKDIR/small.txt" >/dev/null
  aws --endpoint-url "$S3_ENDPOINT" s3api delete-objects --bucket "$S3_BUCKET" --delete "Objects=[{Key=$DELETE_A_KEY},{Key=$DELETE_B_KEY}],Quiet=false" >/dev/null
  aws --endpoint-url "$S3_ENDPOINT" s3 rm "s3://$S3_BUCKET/$SMALL_KEY" >/dev/null
  aws --endpoint-url "$S3_ENDPOINT" s3 rm "s3://$S3_BUCKET/$BIG_KEY" >/dev/null
  aws --endpoint-url "$S3_ENDPOINT" s3 rm "s3://$S3_BUCKET/$COPY_KEY" >/dev/null
}

mc_smoke() {
  require_cmd mc || return 0
  log "MinIO mc: upload, stat, cat, list, remove"
  alias_name="hfgw-$$"
  mc alias set "$alias_name" "$S3_ENDPOINT" "$S3_ACCESS_KEY" "$S3_SECRET_KEY" >/dev/null
  mc cp "$WORKDIR/small.txt" "$alias_name/$S3_BUCKET/$SMALL_KEY" >/dev/null
  mc stat "$alias_name/$S3_BUCKET/$SMALL_KEY" >/dev/null
  mc cat "$alias_name/$S3_BUCKET/$SMALL_KEY" > "$WORKDIR/mc-cat.txt"
  cmp "$WORKDIR/small.txt" "$WORKDIR/mc-cat.txt"
  mc ls "$alias_name/$S3_BUCKET/$PREFIX/" >/dev/null
  mc rm "$alias_name/$S3_BUCKET/$SMALL_KEY" >/dev/null
  mc alias rm "$alias_name" >/dev/null || true
}

rclone_smoke() {
  require_cmd rclone || return 0
  log "rclone: copy, lsf, cat, deletefile"
  export RCLONE_CONFIG_HFGW_TYPE=s3
  export RCLONE_CONFIG_HFGW_PROVIDER=Other
  export RCLONE_CONFIG_HFGW_ACCESS_KEY_ID="$S3_ACCESS_KEY"
  export RCLONE_CONFIG_HFGW_SECRET_ACCESS_KEY="$S3_SECRET_KEY"
  export RCLONE_CONFIG_HFGW_ENDPOINT="$S3_ENDPOINT"
  export RCLONE_CONFIG_HFGW_REGION="${S3_REGION:-us-east-1}"
  export RCLONE_CONFIG_HFGW_NO_CHECK_BUCKET=true
  rclone copyto "$WORKDIR/small.txt" "hfgw:$S3_BUCKET/$SMALL_KEY"
  rclone lsf "hfgw:$S3_BUCKET/$PREFIX/" | grep -F "small.txt" >/dev/null
  rclone cat "hfgw:$S3_BUCKET/$SMALL_KEY" > "$WORKDIR/rclone-cat.txt"
  cmp "$WORKDIR/small.txt" "$WORKDIR/rclone-cat.txt"
  rclone deletefile "hfgw:$S3_BUCKET/$SMALL_KEY"
}

s3fs_smoke() {
  if [[ "${RUN_S3FS:-0}" != "1" ]]; then
    printf 'skip: s3fs mount smoke disabled; set RUN_S3FS=1 to enable\n' >&2
    return 0
  fi
  require_cmd s3fs || return 0
  require_cmd fusermount || require_cmd fusermount3 || return 0
  log "s3fs: mount, write, read, delete, unmount"
  passfile="$WORKDIR/s3fs.passwd"
  mountdir="$WORKDIR/mnt"
  mkdir -p "$mountdir"
  chmod 700 "$WORKDIR"
  printf '%s:%s\n' "$S3_ACCESS_KEY" "$S3_SECRET_KEY" > "$passfile"
  chmod 600 "$passfile"
  s3fs "$S3_BUCKET" "$mountdir" -o passwd_file="$passfile" -o url="$S3_ENDPOINT" -o use_path_request_style -o nonempty -o dbglevel=warn
  trap 'fusermount -u "$mountdir" 2>/dev/null || fusermount3 -u "$mountdir" 2>/dev/null || true; cleanup' EXIT
  mkdir -p "$mountdir/$PREFIX"
  cp "$WORKDIR/small.txt" "$mountdir/$SMALL_KEY"
  cmp "$WORKDIR/small.txt" "$mountdir/$SMALL_KEY"
  rm -f "$mountdir/$SMALL_KEY"
  fusermount -u "$mountdir" 2>/dev/null || fusermount3 -u "$mountdir" 2>/dev/null || true
}

write_fixture_files
check_gateway_bucket
curl_smoke
aws_cli_smoke
mc_smoke
rclone_smoke
s3fs_smoke
log "compatibility smoke completed for prefix $PREFIX"
