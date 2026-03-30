package server

import (
	"context"
	"errors"
	"io"
	"time"
)

var errNotFound = errors.New("object not found")

type ObjectInfo struct {
	Key          string
	Size         int64
	ModTime      time.Time
	ETag         string
	StorageClass string
}

type Storage interface {
	ListObjects(ctx context.Context, prefix string) ([]ObjectInfo, error)
	PutObject(ctx context.Context, key string, body io.Reader) error
	GetObject(ctx context.Context, key string) (io.ReadCloser, ObjectInfo, error)
	HeadObject(ctx context.Context, key string) (ObjectInfo, error)
	DeleteObject(ctx context.Context, key string) error
}

func NewStorageFromEnv() Storage {
	switch getenv("STORAGE_BACKEND", "local") {
	case "hf":
		return newHFPlaceholderStorage()
	default:
		return newLocalStorage(dataRoot())
	}
}
