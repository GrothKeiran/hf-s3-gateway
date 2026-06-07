package server

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	gatewaySignQuery    = "gateway-presign"
	gatewayExpiresQuery = "X-HF-S3-Expires"
	gatewaySigQuery     = "X-HF-S3-Signature"
)

func gatewayPresignRequested(r *http.Request) bool {
	_, ok := r.URL.Query()[gatewaySignQuery]
	return ok
}

func gatewaySignedURL(r *http.Request, ttlSeconds int64) (string, int64, error) {
	if ttlSeconds <= 0 {
		ttlSeconds = int64EnvLocal("GATEWAY_PRESIGN_DEFAULT_EXPIRES", 3600)
	}
	maxTTL := int64EnvLocal("GATEWAY_PRESIGN_MAX_EXPIRES", 7*24*3600)
	if maxTTL > 0 && ttlSeconds > maxTTL {
		ttlSeconds = maxTTL
	}
	expires := time.Now().UTC().Unix() + ttlSeconds
	path := r.URL.EscapedPath()
	if path == "" {
		path = r.URL.Path
	}
	sig := signGatewayDownload(downloadSignMethod(r.Method), r.URL.Path, expires)
	q := url.Values{}
	q.Set(gatewayExpiresQuery, strconv.FormatInt(expires, 10))
	q.Set(gatewaySigQuery, sig)
	return publicBaseURL(r) + path + "?" + q.Encode(), expires, nil
}

func isGatewaySignedDownload(r *http.Request) bool {
	if !isObjectReadRequest(r) {
		return false
	}
	q := r.URL.Query()
	expRaw := q.Get(gatewayExpiresQuery)
	sig := strings.ToLower(strings.TrimSpace(q.Get(gatewaySigQuery)))
	if expRaw == "" || sig == "" {
		return false
	}
	expires, err := strconv.ParseInt(expRaw, 10, 64)
	if err != nil || expires < time.Now().UTC().Unix() {
		return false
	}
	expected := signGatewayDownload(downloadSignMethod(r.Method), r.URL.Path, expires)
	return subtle.ConstantTimeCompare([]byte(expected), []byte(sig)) == 1
}

func isPublicObjectRead(r *http.Request) bool {
	return hfSDKEnabled("PUBLIC_READ", false) && isObjectReadRequest(r)
}

func isObjectReadRequest(r *http.Request) bool {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return false
	}
	bucket, key := objectPathParts(r.URL.Path)
	return bucket == expectedBucket() && key != ""
}

func objectPathParts(p string) (string, string) {
	p = strings.TrimPrefix(p, "/")
	bucket, key, ok := strings.Cut(p, "/")
	if !ok {
		return bucket, ""
	}
	return bucket, cleanKey(key)
}

func signGatewayDownload(method, path string, expires int64) string {
	msg := strings.Join([]string{method, canonicalURI(path), strconv.FormatInt(expires, 10)}, "\n")
	mac := hmac.New(sha256.New, []byte(gatewaySignSecret()))
	_, _ = mac.Write([]byte(msg))
	return hex.EncodeToString(mac.Sum(nil))
}

func gatewaySignSecret() string {
	if v := getenv("GATEWAY_SIGN_SECRET", ""); v != "" {
		return v
	}
	return getenv("S3_SECRET_KEY", "change-me")
}

func downloadSignMethod(method string) string {
	if method == http.MethodHead {
		return http.MethodGet
	}
	return method
}

func publicBaseURL(r *http.Request) string {
	if base := strings.TrimRight(getenv("PUBLIC_BASE_URL", ""), "/"); base != "" {
		return base
	}
	scheme := r.Header.Get("X-Forwarded-Proto")
	if scheme == "" {
		scheme = "http"
		if r.TLS != nil {
			scheme = "https"
		}
	}
	host := r.Header.Get("X-Forwarded-Host")
	if host == "" {
		host = r.Host
	}
	return fmt.Sprintf("%s://%s", scheme, host)
}

func int64EnvLocal(key string, fallback int64) int64 {
	raw := strings.TrimSpace(getenv(key, ""))
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v < 0 {
		return fallback
	}
	return v
}
