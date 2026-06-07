package server

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestAWSChunkedReaderDecodesPayload(t *testing.T) {
	encoded := "5;chunk-signature=abc\r\nhello\r\n6;chunk-signature=def\r\n world\r\n0;chunk-signature=done\r\n\r\n"
	out, err := io.ReadAll(newAWSChunkedReader(strings.NewReader(encoded)))
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != "hello world" {
		t.Fatalf("unexpected decoded payload: %q", string(out))
	}
}

func TestPutObjectDecodesAWSChunkedPayload(t *testing.T) {
	r := newTestRouter(t)
	body := "5;chunk-signature=abc\r\nhello\r\n6;chunk-signature=def\r\n world\r\n0;chunk-signature=done\r\n\r\n"
	w := performRequest(r, http.MethodPut, "/bucket/chunked.txt", body, map[string]string{"Content-Encoding": "aws-chunked"})
	if w.Code != http.StatusOK {
		t.Fatalf("PUT status=%d body=%s", w.Code, w.Body.String())
	}
	w = performRequest(r, http.MethodGet, "/bucket/chunked.txt", "", nil)
	if w.Code != http.StatusOK || w.Body.String() != "hello world" {
		t.Fatalf("GET status=%d body=%q", w.Code, w.Body.String())
	}
}
