# syntax=docker/dockerfile:1.7
FROM golang:1.22-alpine AS builder
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build -trimpath -ldflags='-s -w' -o /out/hf-s3-gateway ./cmd/server

FROM alpine:3.20
RUN apk add --no-cache python3 py3-pip ca-certificates && \
    python3 -m pip install --no-cache-dir --break-system-packages 'huggingface_hub[cli]' && \
    adduser -D -H -u 10001 appuser
WORKDIR /app
COPY --from=builder /out/hf-s3-gateway /usr/local/bin/hf-s3-gateway
EXPOSE 9000
USER appuser
ENTRYPOINT ["/usr/local/bin/hf-s3-gateway"]
