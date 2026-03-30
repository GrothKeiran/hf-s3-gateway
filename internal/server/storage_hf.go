package server

import (
	"context"
	"fmt"
	"io"
	"strings"
)

type hfPlaceholderStorage struct{}

func newHFPlaceholderStorage() Storage {
	return &hfPlaceholderStorage{}
}

func (s *hfPlaceholderStorage) ListObjects(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	_ = ctx
	_ = prefix
	return nil, fmt.Errorf("hf backend not implemented yet")
}

func (s *hfPlaceholderStorage) PutObject(ctx context.Context, key string, body io.Reader) error {
	_ = ctx
	_ = key
	_, _ = io.Copy(io.Discard, body)
	return fmt.Errorf("hf backend not implemented yet")
}

func (s *hfPlaceholderStorage) GetObject(ctx context.Context, key string) (io.ReadCloser, ObjectInfo, error) {
	_ = ctx
	_ = key
	return nil, ObjectInfo{}, fmt.Errorf("hf backend not implemented yet")
}

func (s *hfPlaceholderStorage) HeadObject(ctx context.Context, key string) (ObjectInfo, error) {
	_ = ctx
	_ = key
	return ObjectInfo{}, fmt.Errorf("hf backend not implemented yet")
}

func (s *hfPlaceholderStorage) DeleteObject(ctx context.Context, key string) error {
	_ = ctx
	_ = key
	return fmt.Errorf("hf backend not implemented yet")
}

func isHFNotImplemented(err error) bool {
	return err != nil && strings.Contains(err.Error(), "hf backend not implemented yet")
}
