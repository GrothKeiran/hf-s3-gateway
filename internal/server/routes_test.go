package server

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func newTestRouter(t *testing.T) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	t.Setenv("STORAGE_BACKEND", "local")
	t.Setenv("DATA_DIR", t.TempDir())
	t.Setenv("HF_BUCKET", "bucket")
	t.Setenv("S3_ACCESS_KEY", "")
	t.Setenv("S3_SECRET_KEY", "")
	r := gin.New()
	RegisterRoutes(r)
	return r
}

func performRequest(r http.Handler, method, target string, body string, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func putTestObject(t *testing.T, r http.Handler, key, body string) {
	t.Helper()
	w := performRequest(r, http.MethodPut, "/bucket/"+key, body, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("PUT %s status=%d body=%s", key, w.Code, w.Body.String())
	}
}

func TestHealthAndReadyEndpoints(t *testing.T) {
	r := newTestRouter(t)
	w := performRequest(r, http.MethodGet, "/healthz", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("healthz status=%d body=%s", w.Code, w.Body.String())
	}
	w = performRequest(r, http.MethodGet, "/readyz", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("readyz status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestListObjectsV2PaginationDoesNotSkipKeys(t *testing.T) {
	r := newTestRouter(t)
	putTestObject(t, r, "a.txt", "a")
	putTestObject(t, r, "b.txt", "b")
	putTestObject(t, r, "c.txt", "c")

	w := performRequest(r, http.MethodGet, "/bucket?list-type=2&max-keys=2", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("first list status=%d body=%s", w.Code, w.Body.String())
	}
	var first listBucketResult
	if err := xml.Unmarshal(w.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}
	if !first.IsTruncated || first.NextContinuationToken == "" {
		t.Fatalf("expected truncated page with token: %+v", first)
	}
	if len(first.Contents) != 2 || first.Contents[0].Key != "a.txt" || first.Contents[1].Key != "b.txt" {
		t.Fatalf("unexpected first page contents: %+v", first.Contents)
	}

	w = performRequest(r, http.MethodGet, "/bucket?list-type=2&continuation-token="+url.QueryEscape(first.NextContinuationToken), "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("second list status=%d body=%s", w.Code, w.Body.String())
	}
	var second listBucketResult
	if err := xml.Unmarshal(w.Body.Bytes(), &second); err != nil {
		t.Fatal(err)
	}
	if len(second.Contents) != 1 || second.Contents[0].Key != "c.txt" {
		t.Fatalf("unexpected second page contents: %+v", second.Contents)
	}
}

func TestRangeGetAndHeadObject(t *testing.T) {
	r := newTestRouter(t)
	putTestObject(t, r, "file.txt", "hello world")

	w := performRequest(r, http.MethodGet, "/bucket/file.txt", "", nil)
	if w.Code != http.StatusOK || w.Header().Get("X-HF-S3-Get-Mode") != "proxy" {
		t.Fatalf("plain get status=%d mode=%q", w.Code, w.Header().Get("X-HF-S3-Get-Mode"))
	}

	w = performRequest(r, http.MethodGet, "/bucket/file.txt", "", map[string]string{"Range": "bytes=6-10"})
	if w.Code != http.StatusPartialContent {
		t.Fatalf("range get status=%d body=%s", w.Code, w.Body.String())
	}
	if w.Body.String() != "world" {
		t.Fatalf("unexpected range body: %q", w.Body.String())
	}
	if got := w.Header().Get("Content-Range"); got != "bytes 6-10/11" {
		t.Fatalf("unexpected content-range: %q", got)
	}
	if got := w.Header().Get("Accept-Ranges"); got != "bytes" {
		t.Fatalf("unexpected accept-ranges: %q", got)
	}

	w = performRequest(r, http.MethodHead, "/bucket/file.txt", "", map[string]string{"Range": "bytes=0-4"})
	if w.Code != http.StatusPartialContent {
		t.Fatalf("range head status=%d", w.Code)
	}
	if got := w.Header().Get("Content-Length"); got != "5" {
		t.Fatalf("unexpected head content-length: %q", got)
	}
}

func TestDeleteObjects(t *testing.T) {
	r := newTestRouter(t)
	putTestObject(t, r, "a.txt", "a")
	putTestObject(t, r, "b.txt", "b")

	body := `<Delete><Object><Key>a.txt</Key></Object><Object><Key>b.txt</Key></Object></Delete>`
	w := performRequest(r, http.MethodPost, "/bucket?delete", body, map[string]string{"Content-Type": "application/xml"})
	if w.Code != http.StatusOK {
		t.Fatalf("delete objects status=%d body=%s", w.Code, w.Body.String())
	}
	for _, key := range []string{"a.txt", "b.txt"} {
		w = performRequest(r, http.MethodHead, "/bucket/"+key, "", nil)
		if w.Code != http.StatusNotFound {
			t.Fatalf("expected %s deleted, status=%d", key, w.Code)
		}
	}
	putTestObject(t, r, "c.txt", "c")
	w = performRequest(r, http.MethodPost, "/bucket/?delete=", `<Delete><Object><Key>c.txt</Key></Object></Delete>`, map[string]string{"Content-Type": "application/xml"})
	if w.Code != http.StatusOK {
		t.Fatalf("trailing slash delete objects status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestBucketSubresourcesAndValidation(t *testing.T) {
	r := newTestRouter(t)
	w := performRequest(r, http.MethodGet, "/bucket?location", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("location status=%d body=%s", w.Code, w.Body.String())
	}
	w = performRequest(r, http.MethodGet, "/bucket/?location=", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("trailing slash location status=%d body=%s", w.Code, w.Body.String())
	}
	w = performRequest(r, http.MethodGet, "/wrong-bucket?list-type=2", "", nil)
	if w.Code != http.StatusNotFound {
		t.Fatalf("wrong bucket status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestCopyObject(t *testing.T) {
	r := newTestRouter(t)
	putTestObject(t, r, "src.txt", "copy me")

	w := performRequest(r, http.MethodPut, "/bucket/dst.txt", "", map[string]string{"X-Amz-Copy-Source": "/bucket/src.txt"})
	if w.Code != http.StatusOK {
		t.Fatalf("copy status=%d body=%s", w.Code, w.Body.String())
	}
	w = performRequest(r, http.MethodGet, "/bucket/dst.txt", "", nil)
	if w.Code != http.StatusOK || w.Body.String() != "copy me" {
		t.Fatalf("copied object status=%d body=%q", w.Code, w.Body.String())
	}
}

func TestMultipartListPartsAndComplete(t *testing.T) {
	r := newTestRouter(t)
	w := performRequest(r, http.MethodPost, "/bucket/multipart.bin?uploads", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("init multipart status=%d body=%s", w.Code, w.Body.String())
	}
	var initRes initiateMultipartUploadResult
	if err := xml.Unmarshal(w.Body.Bytes(), &initRes); err != nil {
		t.Fatal(err)
	}
	if initRes.UploadID == "" {
		t.Fatalf("missing upload id: %+v", initRes)
	}

	partURL := "/bucket/multipart.bin?uploadId=" + url.QueryEscape(initRes.UploadID) + "&partNumber=1"
	w = performRequest(r, http.MethodPut, partURL, "hello", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("upload part status=%d body=%s", w.Code, w.Body.String())
	}
	etag := w.Header().Get("ETag")
	if etag == "" {
		t.Fatal("missing part etag")
	}

	w = performRequest(r, http.MethodGet, "/bucket/multipart.bin?uploadId="+url.QueryEscape(initRes.UploadID), "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("list parts status=%d body=%s", w.Code, w.Body.String())
	}
	var parts listPartsResult
	if err := xml.Unmarshal(w.Body.Bytes(), &parts); err != nil {
		t.Fatal(err)
	}
	if len(parts.Parts) != 1 || parts.Parts[0].PartNumber != 1 {
		t.Fatalf("unexpected parts: %+v", parts.Parts)
	}

	w = performRequest(r, http.MethodGet, "/bucket?uploads", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("list uploads status=%d body=%s", w.Code, w.Body.String())
	}
	var uploads listMultipartUploadsResult
	if err := xml.Unmarshal(w.Body.Bytes(), &uploads); err != nil {
		t.Fatal(err)
	}
	if len(uploads.Uploads) != 1 || uploads.Uploads[0].UploadID != initRes.UploadID {
		t.Fatalf("unexpected uploads: %+v", uploads.Uploads)
	}

	completeBody := `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>` + etag + `</ETag></Part></CompleteMultipartUpload>`
	w = performRequest(r, http.MethodPost, "/bucket/multipart.bin?uploadId="+url.QueryEscape(initRes.UploadID), completeBody, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("complete multipart status=%d body=%s", w.Code, w.Body.String())
	}
	w = performRequest(r, http.MethodGet, "/bucket/multipart.bin", "", nil)
	if w.Code != http.StatusOK || w.Body.String() != "hello" {
		t.Fatalf("completed object status=%d body=%q", w.Code, w.Body.String())
	}
}

func TestLocalStorageRejectsTraversal(t *testing.T) {
	s := newLocalStorage(t.TempDir())
	err := s.PutObject(context.Background(), "../evil.txt", bytes.NewReader([]byte("x")))
	if !errors.Is(err, errInvalidKey) {
		t.Fatalf("expected errInvalidKey, got %v", err)
	}
}
