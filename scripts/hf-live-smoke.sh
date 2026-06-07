#!/usr/bin/env bash
set -euo pipefail

: "${HF_TOKEN:?set HF_TOKEN for a real Hugging Face account with bucket access}"
: "${HF_NAMESPACE:?set HF_NAMESPACE}"
: "${HF_BUCKET:?set HF_BUCKET}"
: "${S3_ACCESS_KEY:?set S3_ACCESS_KEY}"
: "${S3_SECRET_KEY:?set S3_SECRET_KEY}"

APP_ADDR="${APP_ADDR:-127.0.0.1:19100}"
S3_ENDPOINT="${S3_ENDPOINT:-http://$APP_ADDR}"
DATA_DIR="${DATA_DIR:-$(mktemp -d)}"
GATEWAY_BIN="${GATEWAY_BIN:-}"
WORKDIR="$(mktemp -d)"
PID=""

cleanup() {
  if [[ -n "$PID" ]]; then
    kill "$PID" 2>/dev/null || true
    wait "$PID" 2>/dev/null || true
  fi
  rm -rf "$WORKDIR"
  if [[ -z "${KEEP_DATA_DIR:-}" ]]; then
    rm -rf "$DATA_DIR"
  fi
}
trap cleanup EXIT

log() { printf '\n==> %s\n' "$*"; }
fail() { printf 'error: %s\n' "$*" >&2; exit 1; }

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "$1 is required"
}

build_gateway_if_needed() {
  if [[ -n "$GATEWAY_BIN" ]]; then
    [[ -x "$GATEWAY_BIN" ]] || fail "GATEWAY_BIN is not executable: $GATEWAY_BIN"
    return
  fi
  require_cmd go
  GATEWAY_BIN="$WORKDIR/hf-s3-gateway"
  go build -trimpath -o "$GATEWAY_BIN" ./cmd/server
}

wait_http_ok() {
  local url="$1"
  for _ in $(seq 1 90); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

start_gateway() {
  local redirect_required="$1"
  log "starting HF backend gateway on $APP_ADDR"
  STORAGE_BACKEND=hf \
  APP_ADDR="$APP_ADDR" \
  DATA_DIR="$DATA_DIR" \
  HF_NAMESPACE="$HF_NAMESPACE" \
  HF_BUCKET="$HF_BUCKET" \
  HF_TOKEN="$HF_TOKEN" \
  S3_ACCESS_KEY="$S3_ACCESS_KEY" \
  S3_SECRET_KEY="$S3_SECRET_KEY" \
  HF_REDIRECT_GET=true \
  HF_REDIRECT_GET_REQUIRED="$redirect_required" \
  "$GATEWAY_BIN" >"$WORKDIR/gateway.log" 2>&1 &
  PID=$!
  wait_http_ok "$S3_ENDPOINT/healthz" || {
    tail -200 "$WORKDIR/gateway.log" >&2 || true
    fail "gateway did not become healthy"
  }
  curl -fsS "$S3_ENDPOINT/readyz" || {
    tail -200 "$WORKDIR/gateway.log" >&2 || true
    fail "gateway readiness failed"
  }
}

stop_gateway() {
  if [[ -n "$PID" ]]; then
    kill "$PID" 2>/dev/null || true
    wait "$PID" 2>/dev/null || true
    PID=""
  fi
}

proxy_and_client_smoke() {
  log "running full client compatibility smoke against real HF backend"
  S3_ENDPOINT="$S3_ENDPOINT" \
  S3_BUCKET="$HF_BUCKET" \
  S3_ACCESS_KEY="$S3_ACCESS_KEY" \
  S3_SECRET_KEY="$S3_SECRET_KEY" \
  ./scripts/compat-smoke.sh
}

hf_cli_visibility_smoke() {
  require_cmd hf
  log "checking uploaded objects are visible through hf CLI"
  local probe_key="hf-live-smoke/$(date +%s)-$$.txt"
  printf 'hf live smoke\n' > "$WORKDIR/hf-live.txt"
  curl -fsS -u "$S3_ACCESS_KEY:$S3_SECRET_KEY" -X PUT --data-binary "@$WORKDIR/hf-live.txt" "$S3_ENDPOINT/$HF_BUCKET/$probe_key" >/dev/null
  hf buckets ls "hf://buckets/$HF_NAMESPACE/$HF_BUCKET/$(dirname "$probe_key")" | grep -F "$(basename "$probe_key")" >/dev/null
  curl -fsS -u "$S3_ACCESS_KEY:$S3_SECRET_KEY" -X DELETE "$S3_ENDPOINT/$HF_BUCKET/$probe_key" >/dev/null
}

hf_redirect_probe() {
  log "probing HF signed redirect mode"
  local key="hf-live-smoke/redirect-$(date +%s)-$$.txt"
  printf 'redirect probe\n' > "$WORKDIR/redirect.txt"
  curl -fsS -u "$S3_ACCESS_KEY:$S3_SECRET_KEY" -X PUT --data-binary "@$WORKDIR/redirect.txt" "$S3_ENDPOINT/$HF_BUCKET/$key" >/dev/null
  local headers="$WORKDIR/redirect.headers"
  local code
  code="$(curl -sS -u "$S3_ACCESS_KEY:$S3_SECRET_KEY" -o /dev/null -D "$headers" -w '%{http_code}' "$S3_ENDPOINT/$HF_BUCKET/$key")"
  if [[ "$code" == "307" || "$code" == "302" ]]; then
    grep -i '^Location: https\?://' "$headers" >/dev/null || fail "redirect response did not include an HTTP Location"
    printf 'HF redirect signed URL: available\n'
  else
    if [[ "${REQUIRE_HF_REDIRECT:-0}" == "1" ]]; then
      cat "$headers" >&2
      fail "HF redirect signed URL required but response code was $code"
    fi
    printf 'HF redirect signed URL: unavailable, gateway proxy path returned HTTP %s\n' "$code"
  fi
  curl -fsS -u "$S3_ACCESS_KEY:$S3_SECRET_KEY" -X DELETE "$S3_ENDPOINT/$HF_BUCKET/$key" >/dev/null
}

main() {
  require_cmd curl
  require_cmd python3
  build_gateway_if_needed
  start_gateway "${REQUIRE_HF_REDIRECT:-0}"
  proxy_and_client_smoke
  hf_cli_visibility_smoke
  hf_redirect_probe
  log "real HF backend smoke completed"
}

main "$@"
