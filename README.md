# hf-s3-gateway

一个面向 **Hugging Face Buckets** 的轻量级 **S3 兼容网关**，用于让 OpenList 等只支持 S3 的工具，通过标准化的 S3 接口访问 HF Bucket。

> 目标场景：将 `hf://buckets/<your-namespace>/<your-bucket>` 暴露为 OpenList 可接入的 S3 存储。

---

## 功能概览

当前已实现/可用的能力：

- 单逻辑 Bucket 暴露
- `ListBuckets`
- `HeadBucket`
- `ListObjectsV2`
- `PutObject`
- `GetObject`
- `HeadObject`
- `DeleteObject`
- `Basic Auth`
- `AWS Signature V4` 请求校验（兼容 OpenList 等 S3 客户端）
- `/healthz` 健康检查接口
- Multipart Upload 基础支持：
  - `CreateMultipartUpload`
  - `UploadPart`
  - `CompleteMultipartUpload`

---

## 当前状态

项目已完成 MVP 阶段，当前重点是继续提升与 OpenList 的兼容性，以及优化大文件上传体验。

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
  hf-s3-gateway:
    image: ghcr.io/grothkeiran/hf-s3-gateway:latest
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
    volumes:
      - ./data:/data
```

注意：

- `./data` 目录需要容器用户可写，否则 HF 临时目录（如 `/data/.hf-tmp`）创建失败会导致列目录/上传报错
- 如果使用挂载目录，确保权限允许容器内用户写入
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
