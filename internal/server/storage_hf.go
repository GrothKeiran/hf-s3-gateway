package server

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type hfCLIStorage struct {
	cli *hfCLI
}

func newHFPlaceholderStorage() Storage {
	return &hfCLIStorage{cli: newHFCLIFromEnv()}
}

func (s *hfCLIStorage) ListObjects(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	if err := s.cli.ensureReady(); err != nil {
		return nil, err
	}
	_ = ctx
	_ = prefix
	return nil, fmt.Errorf("hf backend list not implemented yet")
}

func (s *hfCLIStorage) PutObject(ctx context.Context, key string, body io.Reader) error {
	if err := s.cli.ensureReady(); err != nil {
		_, _ = io.Copy(io.Discard, body)
		return err
	}
	tmpDir := s.cli.workDir
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		_, _ = io.Copy(io.Discard, body)
		return err
	}
	f, err := os.CreateTemp(tmpDir, "upload-*")
	if err != nil {
		_, _ = io.Copy(io.Discard, body)
		return err
	}
	tmpPath := f.Name()
	defer os.Remove(tmpPath)
	defer f.Close()
	if _, err := io.Copy(f, body); err != nil {
		return err
	}
	_, err = s.cli.run(ctx, "buckets", "cp", tmpPath, s.cli.bucketURI(key))
	return err
}

func (s *hfCLIStorage) GetObject(ctx context.Context, key string) (io.ReadCloser, ObjectInfo, error) {
	if err := s.cli.ensureReady(); err != nil {
		return nil, ObjectInfo{}, err
	}
	tmpDir := s.cli.workDir
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return nil, ObjectInfo{}, err
	}
	localPath := filepath.Join(tmpDir, fmt.Sprintf("download-%d-%s", time.Now().UnixNano(), strings.ReplaceAll(cleanKey(key), "/", "_")))
	_, err := s.cli.run(ctx, "buckets", "cp", s.cli.bucketURI(key), localPath)
	if err != nil {
		return nil, ObjectInfo{}, err
	}
	f, err := os.Open(localPath)
	if err != nil {
		return nil, ObjectInfo{}, err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, ObjectInfo{}, err
	}
	return &tempFileReadCloser{File: f, path: localPath}, ObjectInfo{
		Key:          cleanKey(key),
		Size:         st.Size(),
		ModTime:      st.ModTime().UTC(),
		ETag:         fmt.Sprintf("\"%x-%d\"", st.ModTime().UnixNano(), st.Size()),
		StorageClass: "STANDARD",
	}, nil
}

func (s *hfCLIStorage) HeadObject(ctx context.Context, key string) (ObjectInfo, error) {
	body, meta, err := s.GetObject(ctx, key)
	if err != nil {
		return ObjectInfo{}, err
	}
	_ = body.Close()
	return meta, nil
}

func (s *hfCLIStorage) DeleteObject(ctx context.Context, key string) error {
	if err := s.cli.ensureReady(); err != nil {
		return err
	}
	_, err := s.cli.run(ctx, "buckets", "rm", s.cli.bucketURI(key))
	return err
}

func isHFNotImplemented(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "hf backend not implemented yet") || strings.Contains(msg, "hf backend list not implemented yet")
}

type tempFileReadCloser struct {
	*os.File
	path string
}

func (t *tempFileReadCloser) Close() error {
	err1 := t.File.Close()
	err2 := os.Remove(t.path)
	if err1 != nil {
		return err1
	}
	return err2
}
