package server

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

func TestCORSPreflightBypassesAuth(t *testing.T) {
	r := newAuthTestRouter(t)
	w := performRequest(r, http.MethodOptions, "/bucket/video.mp4", "", map[string]string{
		"Origin":                         "https://preview.example",
		"Access-Control-Request-Headers": "Range,Authorization",
	})
	if w.Code != http.StatusNoContent {
		t.Fatalf("OPTIONS status=%d body=%s", w.Code, w.Body.String())
	}
	if got := w.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("unexpected allow origin: %q", got)
	}
	if got := w.Header().Get("Access-Control-Allow-Headers"); got != "Range,Authorization" {
		t.Fatalf("unexpected allow headers: %q", got)
	}
}

func TestPreviewHeadersForMediaObjects(t *testing.T) {
	r := newTestRouter(t)
	t.Setenv("OBJECT_CACHE_CONTROL", "public, max-age=60")
	w := performRequest(r, http.MethodPut, "/bucket/%E7%94%B5%E5%BD%B1%2001.mkv", "video", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("PUT status=%d body=%s", w.Code, w.Body.String())
	}

	w = performRequest(r, http.MethodGet, "/bucket/%E7%94%B5%E5%BD%B1%2001.mkv", "", map[string]string{"Origin": "https://preview.example"})
	if w.Code != http.StatusOK {
		t.Fatalf("GET status=%d body=%s", w.Code, w.Body.String())
	}
	if got := w.Header().Get("Content-Type"); got != "video/x-matroska" {
		t.Fatalf("unexpected content-type: %q", got)
	}
	if got := w.Header().Get("Content-Disposition"); !strings.HasPrefix(got, "inline;") || !strings.Contains(got, "%E7%94%B5%E5%BD%B1%2001.mkv") {
		t.Fatalf("unexpected content-disposition: %q", got)
	}
	if got := w.Header().Get("Cache-Control"); got != "public, max-age=60" {
		t.Fatalf("unexpected cache-control: %q", got)
	}
	if got := w.Header().Get("Access-Control-Expose-Headers"); !strings.Contains(got, "Content-Range") {
		t.Fatalf("unexpected expose headers: %q", got)
	}
}

func TestGatewaySignedDownloadURLSupportsRangeWithoutAuth(t *testing.T) {
	r := newAuthTestRouter(t)
	put := performRequest(r, http.MethodPut, "/bucket/video.mp4", "0123456789", map[string]string{"Authorization": basicAuthHeader("openlist", "secret")})
	if put.Code != http.StatusOK {
		t.Fatalf("PUT status=%d body=%s", put.Code, put.Body.String())
	}

	w := performRequest(r, http.MethodGet, "/bucket/video.mp4?gateway-presign&expires=120", "", map[string]string{"Authorization": basicAuthHeader("openlist", "secret")})
	if w.Code != http.StatusOK {
		t.Fatalf("presign status=%d body=%s", w.Code, w.Body.String())
	}
	var res struct {
		URL string `json:"url"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatal(err)
	}
	idx := strings.Index(res.URL, "/bucket/video.mp4?")
	if idx < 0 {
		t.Fatalf("unexpected signed url: %s", res.URL)
	}
	target := res.URL[idx:]
	w = performRequest(r, http.MethodGet, target, "", map[string]string{"Range": "bytes=2-5"})
	if w.Code != http.StatusPartialContent || w.Body.String() != "2345" {
		t.Fatalf("signed range status=%d body=%q", w.Code, w.Body.String())
	}
}

func TestPublicReadAllowsUnauthenticatedObjectGetOnly(t *testing.T) {
	r := newAuthTestRouter(t)
	put := performRequest(r, http.MethodPut, "/bucket/public.txt", "public", map[string]string{"Authorization": basicAuthHeader("openlist", "secret")})
	if put.Code != http.StatusOK {
		t.Fatalf("PUT status=%d body=%s", put.Code, put.Body.String())
	}
	t.Setenv("PUBLIC_READ", "true")

	w := performRequest(r, http.MethodGet, "/bucket/public.txt", "", nil)
	if w.Code != http.StatusOK || w.Body.String() != "public" {
		t.Fatalf("public get status=%d body=%q", w.Code, w.Body.String())
	}
	w = performRequest(r, http.MethodGet, "/bucket?list-type=2", "", nil)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("public read should not allow list, status=%d", w.Code)
	}
}
