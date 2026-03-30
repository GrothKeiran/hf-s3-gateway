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

## Important

This first scaffold currently uses local disk as the backing store while the S3 compatibility surface is being stabilized.
The next step is swapping the storage backend to Hugging Face Buckets via official APIs/CLI.

## Run

```bash
docker compose up -d
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
