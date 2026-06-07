package server

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestCanonicalQueryStringAWSOrderingAndEncoding(t *testing.T) {
	got := canonicalQueryString("prefix=a+b&list-type=2&delimiter=/&X-Amz-SignedHeaders=host")
	want := "X-Amz-SignedHeaders=host&delimiter=%2F&list-type=2&prefix=a%20b"
	if got != want {
		t.Fatalf("canonical query mismatch\nwant: %s\n got: %s", want, got)
	}
}

func TestValidatePresignExpiry(t *testing.T) {
	now := time.Now().UTC()
	if err := validatePresignExpiry(now.Format("20060102T150405Z"), "60"); err != nil {
		t.Fatalf("fresh presign should validate: %v", err)
	}
	if err := validatePresignExpiry(now.Add(-2*time.Hour).Format("20060102T150405Z"), "60"); err == nil {
		t.Fatal("expired presign should fail")
	}
	if err := validatePresignExpiry(now.Format("20060102T150405Z"), "604801"); err == nil {
		t.Fatal("too-long presign expiry should fail")
	}
}

func TestPresignedSigV4RequestAllowsGetAndRejectsExpired(t *testing.T) {
	r := newAuthTestRouter(t)
	put := performRequest(r, http.MethodPut, "/bucket/signed%20file.txt", "signed-body", map[string]string{"Authorization": basicAuthHeader("openlist", "secret")})
	if put.Code != http.StatusOK {
		t.Fatalf("PUT status=%d body=%s", put.Code, put.Body.String())
	}

	fresh := presignedTarget(t, http.MethodGet, "/bucket/signed%20file.txt", "openlist", "secret", time.Now().UTC(), 300)
	w := performRequest(r, http.MethodGet, fresh, "", nil)
	if w.Code != http.StatusOK || w.Body.String() != "signed-body" {
		t.Fatalf("fresh presigned GET status=%d body=%q", w.Code, w.Body.String())
	}

	expired := presignedTarget(t, http.MethodGet, "/bucket/signed%20file.txt", "openlist", "secret", time.Now().UTC().Add(-2*time.Hour), 60)
	w = performRequest(r, http.MethodGet, expired, "", nil)
	if w.Code != http.StatusForbidden {
		t.Fatalf("expired presigned GET status=%d body=%s", w.Code, w.Body.String())
	}
}

func newAuthTestRouter(t *testing.T) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	t.Setenv("STORAGE_BACKEND", "local")
	t.Setenv("DATA_DIR", t.TempDir())
	t.Setenv("HF_BUCKET", "bucket")
	t.Setenv("S3_ACCESS_KEY", "openlist")
	t.Setenv("S3_SECRET_KEY", "secret")
	r := gin.New()
	RegisterRoutes(r)
	return r
}

func basicAuthHeader(user, pass string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+pass))
}

func presignedTarget(t *testing.T, method, objectPath, accessKey, secret string, at time.Time, expires int) string {
	t.Helper()
	amzDate := at.UTC().Format("20060102T150405Z")
	date := at.UTC().Format("20060102")
	q := url.Values{}
	q.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	q.Set("X-Amz-Credential", fmt.Sprintf("%s/%s/us-east-1/s3/aws4_request", accessKey, date))
	q.Set("X-Amz-Date", amzDate)
	q.Set("X-Amz-Expires", fmt.Sprintf("%d", expires))
	q.Set("X-Amz-SignedHeaders", "host")
	rawQuery := q.Encode()
	req := httptest.NewRequest(method, objectPath+"?"+rawQuery, nil)
	canonicalReq := strings.Join([]string{
		method,
		canonicalURI(req.URL.Path),
		canonicalQueryString(rawQuery),
		"host:" + req.Host + "\n",
		"host",
		"UNSIGNED-PAYLOAD",
	}, "\n")
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		fmt.Sprintf("%s/us-east-1/s3/aws4_request", date),
		hexSHA256([]byte(canonicalReq)),
	}, "\n")
	sig := hex.EncodeToString(hmacSHA256(deriveSigV4Key(secret, date, "us-east-1", "s3"), stringToSign))
	return objectPath + "?" + rawQuery + "&X-Amz-Signature=" + sig
}
