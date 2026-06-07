# hf-s3-gateway

一个面向 **Hugging Face Buckets** 的轻量级 **S3 兼容网关**，用于让 OpenList 等只支持 S3 的工具，通过标准化的 S3 接口访问 HF Bucket。

> 目标场景：将 `hf://buckets/<your-namespace>/<your-bucket>` 暴露为 OpenList 可接入的 S3 存储。

---

## 功能概览

当前已实现/可用的能力：

- 单逻辑 Bucket 暴露
- `ListBuckets`
- `HeadBucket`
- `ListObjects` / `ListObjectsV2`
- `GetBucketLocation` / `GetBucketVersioning` / `GetBucketAcl`
- `PutObject`
- `GetObject` / `HeadObject`
- `Range GET/HEAD`（断点续传、挂载读取）
- `CopyObject`
- `DeleteObject`
- `DeleteObjects` 批量删除
- `Basic Auth`
- `AWS Signature V4` 请求校验（header signing 与 presigned URL）
- `/healthz` 存活检查接口
- `/readyz` 就绪检查接口
- Multipart Upload 支持：
  - `CreateMultipartUpload`
  - `UploadPart`
  - `ListMultipartUploads`
  - `ListParts`
  - `CompleteMultipartUpload`
  - `AbortMultipartUpload`

---

## 当前状态

项目已从 MVP 进入兼容性完善阶段，当前重点是让 OpenList、AWS CLI、rclone、MinIO Client、s3fs 等常见 S3 客户端都能稳定完成上传、删除、列表、断点读取、预签名下载和 multipart 上传。

已完成的重要兼容修复包括：

- `DeleteObject` 补充 `-y`，避免 CLI 交互确认导致删除失败
- `PutObject` 上传流程补齐 `HOME` / `XET_CACHE` 等运行环境
- `ListObjects` 兼容带空格、非 ASCII 文件名
- 修复下载 401 问题（SigV4 query signing）
- 已确认 OpenList 的 S3 上传路径对该网关是**单流上传**，不是前端主动分片到网关

当前已知限制：

- Hugging Face Buckets 暂不支持直接通过 `SignedGetURL` 返回真正可用的 HF 直链签名下载，因此下载仍以网关代理为主
- 大文件上传时，前端可能会先显示 100%，但网关仍在后台把数据同步到 Hugging Face，这段时间会表现为“卡在 100%”

---

## 后端模式

支持两种后端模式：

- `STORAGE_BACKEND=local`：本地磁盘后端（便于调试）
- `STORAGE_BACKEND=hf`：Hugging Face Bucket 后端（推荐实际部署使用）

HF 后端特性：

- 优先使用官方 `hf` CLI
- 当前通过 CLI 适配 `cp` / `rm` / `ls` 等能力
- `ListObjects` 已实现多种输出格式容错解析（优先 JSON，失败时回退文本解析）
- 当 `hf` CLI 缺失或配置不完整时，会明确返回后端错误，避免静默失败

---

## 运行方式

```bash
docker compose up -d
```

启动后检查：

```bash
curl -u '<access-key>:<secret-key>' http://127.0.0.1:9000/healthz
```

如果返回类似：

```json
{"backend":"hf","bucket":"<your-bucket>","namespace":"<your-namespace>","ok":true}
```

说明服务已经正常连接到 Hugging Face Bucket。

---

## Docker Compose 示例

```yaml
services:
  init-perms:
    image: busybox:1.36
    command: sh -c "mkdir -p /data && chown -R 10001:10001 /data"
    volumes:
      - ./data:/data

  hf-s3-gateway:
    image: ghcr.io/grothkeiran/hf-s3-gateway:latest
    depends_on:
      init-perms:
        condition: service_completed_successfully
    ports:
      - "9000:9000"
    environment:
      APP_ADDR: ":9000"
      STORAGE_BACKEND: "hf"
      S3_ACCESS_KEY: "your-access-key"
      S3_SECRET_KEY: "your-secret-key"
      HF_NAMESPACE: "your-namespace"
      HF_BUCKET: "your-bucket"
      HF_TOKEN: "hf_xxx"
      DATA_DIR: "/data"
      HF_WORK_DIR: "/data/.hf-tmp"
    volumes:
      - ./data:/data
```

注意：

- 容器内服务以 UID `10001` 运行；`init-perms` 会在启动前把宿主机 `./data` 修正为该 UID 可写，避免 `/readyz` 出现 `mkdir /data/.hf-tmp: permission denied`。
- 如果不使用上面的 `init-perms`，需要手动执行：`mkdir -p data && sudo chown -R 10001:10001 data`。
- `HF_WORK_DIR` 用于 HF CLI 的 HOME/cache/XET 临时目录；`DATA_DIR` 同时用于 multipart 临时分片，因此二者所在路径都必须可写。
- 不要把真实 `HF_TOKEN`、访问密钥、命名空间、桶名直接写进公开仓库文档

---

## OpenList 配置示例

在 OpenList 中添加 S3 存储时可参考：

- Endpoint: `http://your-host:9000`
- Access Key ID: `your-access-key`
- Secret Access Key: `your-secret-key`
- Bucket: `your-bucket`
- Region: `auto`
- Force Path Style: `true`

---

## 兼容性验证

启动网关后，可以使用内置 smoke 脚本验证真实 S3 客户端行为。该脚本会在指定 bucket 下创建临时前缀，覆盖小文件上传、Range 下载、presigned URL 下载、multipart 上传、复制、批量删除、列表，以及可选 s3fs 挂载。

注意：服务端暴露的 bucket 名来自 `HF_BUCKET`，脚本里的 `S3_BUCKET` 必须与服务端 `HF_BUCKET` 一致。

```bash
export S3_ENDPOINT="http://127.0.0.1:9000"
export S3_BUCKET="your-bucket"
export S3_ACCESS_KEY="your-access-key"
export S3_SECRET_KEY="your-secret-key"

./scripts/compat-smoke.sh
```

脚本会自动跳过本机未安装的客户端：

- `aws`：验证 `s3api`、`s3 cp`、presigned URL、multipart、批量删除
- `mc`：验证 MinIO Client 的上传、读取、删除
- `rclone`：验证挂载类工具常用的 copy/list/cat/delete 行为
- `s3fs`：默认跳过；如需验证 FUSE 挂载，安装 `s3fs` 后设置 `RUN_S3FS=1`

HF 后端实测建议：

1. 准备真实 `HF_NAMESPACE`、`HF_BUCKET`、`HF_TOKEN`，并确保账号对 HF Bucket 有读写权限。
2. 运行 live smoke 脚本，它会启动 `STORAGE_BACKEND=hf` 网关，检查 `/readyz`，运行 `compat-smoke.sh`，并验证对象可通过 `hf` CLI 看到：

```bash
export HF_TOKEN="hf_xxx"
export HF_NAMESPACE="your-namespace"
export HF_BUCKET="your-bucket"
export S3_ACCESS_KEY="your-access-key"
export S3_SECRET_KEY="your-secret-key"

./scripts/hf-live-smoke.sh
```

3. 如果需要把 HF 自身签名 redirect URL 作为硬性要求，额外设置：

```bash
REQUIRE_HF_REDIRECT=1 ./scripts/hf-live-smoke.sh
```

如果 Hugging Face 当前环境无法提供可用 signed redirect URL，脚本会失败；不设置该变量时，网关会验证代理下载与 S3 presigned URL 路径。

4. 大文件验证建议至少覆盖 10MiB 以上对象，以触发 multipart 或客户端分片路径。

s3fs/FUSE 挂载验证需要宿主机具备 `/dev/fuse`、`s3fs`、`fusermount` 或 `fusermount3`：

```bash
export S3_ENDPOINT="http://127.0.0.1:9000"
export S3_BUCKET="your-bucket"
export S3_ACCESS_KEY="your-access-key"
export S3_SECRET_KEY="your-secret-key"

./scripts/s3fs-smoke.sh
```

---

## 生产运行建议

- 容器内已配置 `/healthz` 健康检查，编排系统可使用 `/readyz` 做就绪检查。
- 默认 HTTP server 启用 `ReadHeaderTimeout`、`IdleTimeout` 和优雅停机，避免慢请求占用连接。
- 大文件上传/下载不要设置过短的反向代理超时；HF 后端同步期间客户端可能等待较久。
- 如果需要就绪检查真实访问后端，可设置 `READY_CHECK_STORAGE=true`，但 HF 后端会产生一次列表请求。
- 建议在反向代理层开启 HTTPS，再把内部 HTTP endpoint 暴露给 S3 客户端。

---

## 多架构镜像

GitHub Actions 会构建：

- `linux/amd64`
- `linux/arm64`

发布地址：

- `ghcr.io/grothkeiran/hf-s3-gateway:latest`

---

## English

A lightweight S3-compatible gateway for exposing Hugging Face Buckets to S3-only tools such as OpenList.

Target use case:

- expose `hf://buckets/<your-namespace>/<your-bucket>` through an S3-compatible endpoint

Current focus:

- practical OpenList compatibility
- Hugging Face Bucket backend via official `hf` CLI
- multipart upload support
- better large-file upload behavior

Image:

- `ghcr.io/grothkeiran/hf-s3-gateway:latest`
