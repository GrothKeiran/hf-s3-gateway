# hf-s3-gateway

A minimal S3-compatible gateway intended to let tools like OpenList connect to Hugging Face Buckets through a standard-ish S3 interface.

## Status

Early MVP scaffold.

Current first-pass behavior:
- single logical bucket exposure
- `ListBuckets`
- `HeadBucket`
- `ListObjectsV2`
- `PutObject`
- `GetObject`
- `HeadObject`
- `DeleteObject`
- `Basic Auth` guard for OpenList-style access
- `/healthz` health endpoint

## Important

This first scaffold currently uses local disk as the backing store while the S3 compatibility surface is being stabilized.
A storage abstraction layer is now in place, so the next step is swapping the backend to Hugging Face Buckets via official APIs/CLI without rewriting the HTTP layer.

Current backend modes:
- `STORAGE_BACKEND=local` → working
- `STORAGE_BACKEND=hf` → CLI-backed prototype

HF backend notes:
- prefers the official `hf` CLI
- currently wired for `cp`/`rm` style operations
- `PutObject` / `GetObject` / `HeadObject` / `DeleteObject` are scaffolded through the CLI adapter
- `ListObjects` still needs implementation against real `hf buckets` output behavior
- if the `hf` binary is missing, the service returns a clear backend error instead of failing silently

## Run

```bash
docker compose up -d
```

Then check:

```bash
curl http://127.0.0.1:9000/healthz
```

## OpenList example

- Endpoint: `http://your-host:9000`
- Access Key ID: `openlist`
- Secret Access Key: `change-me`
- Bucket: `your-bucket`
- Region: `auto`
- Force Path Style: `true`

## Multi-arch images

GitHub Actions builds:
- linux/amd64
- linux/arm64

Publishes to:
- `ghcr.io/grothkeiran/hf-s3-gateway:latest`
