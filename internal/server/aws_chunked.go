package server

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

type awsChunkedReader struct {
	r   *bufio.Reader
	buf []byte
	eof bool
}

func objectRequestBody(r *http.Request) io.Reader {
	if isAWSChunkedRequest(r) {
		return newAWSChunkedReader(r.Body)
	}
	return r.Body
}

func isAWSChunkedRequest(r *http.Request) bool {
	if strings.Contains(strings.ToLower(r.Header.Get("Content-Encoding")), "aws-chunked") {
		return true
	}
	payloadHash := strings.ToUpper(r.Header.Get("X-Amz-Content-Sha256"))
	return strings.HasPrefix(payloadHash, "STREAMING-AWS4-HMAC-SHA256-PAYLOAD") || strings.HasPrefix(payloadHash, "STREAMING-UNSIGNED-PAYLOAD")
}

func newAWSChunkedReader(r io.Reader) io.Reader {
	return &awsChunkedReader{r: bufio.NewReader(r)}
}

func (r *awsChunkedReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for len(r.buf) == 0 && !r.eof {
		if err := r.readChunk(); err != nil {
			return 0, err
		}
	}
	if len(r.buf) == 0 && r.eof {
		return 0, io.EOF
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

func (r *awsChunkedReader) readChunk() error {
	line, err := r.r.ReadString('\n')
	if err != nil {
		return err
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		return fmt.Errorf("malformed aws-chunked body: missing chunk header")
	}
	sizeText, _, _ := strings.Cut(line, ";")
	size, err := strconv.ParseInt(sizeText, 16, 64)
	if err != nil || size < 0 {
		return fmt.Errorf("malformed aws-chunked body: invalid chunk size")
	}
	if size == 0 {
		if err := r.discardTrailers(); err != nil {
			return err
		}
		r.eof = true
		return nil
	}
	chunk := make([]byte, size)
	if _, err := io.ReadFull(r.r, chunk); err != nil {
		return err
	}
	if err := expectCRLF(r.r); err != nil {
		return err
	}
	r.buf = chunk
	return nil
}

func (r *awsChunkedReader) discardTrailers() error {
	for {
		line, err := r.r.ReadString('\n')
		if err != nil {
			return err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			return nil
		}
	}
}

func expectCRLF(r *bufio.Reader) error {
	b1, err := r.ReadByte()
	if err != nil {
		return err
	}
	b2, err := r.ReadByte()
	if err != nil {
		return err
	}
	if b1 != '\r' || b2 != '\n' {
		return fmt.Errorf("malformed aws-chunked body: missing chunk terminator")
	}
	return nil
}
