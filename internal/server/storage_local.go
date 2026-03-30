package server

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type localStorage struct {
	root string
}

func newLocalStorage(root string) Storage {
	_ = os.MkdirAll(root, 0o755)
	return &localStorage{root: root}
}

func (s *localStorage) ListObjects(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	_ = ctx
	prefix = strings.TrimPrefix(prefix, "/")
	items := make([]ObjectInfo, 0)
	err := filepath.Walk(s.root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info == nil || info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(s.root, path)
		if err != nil {
			return nil
		}
		key := filepath.ToSlash(rel)
		if prefix != "" && !strings.HasPrefix(key, prefix) {
			return nil
		}
		items = append(items, ObjectInfo{
			Key:          key,
			Size:         info.Size(),
			ModTime:      info.ModTime().UTC(),
			ETag:         fmt.Sprintf("\"%x-%d\"", info.ModTime().UnixNano(), info.Size()),
			StorageClass: "STANDARD",
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Key < items[j].Key })
	return items, nil
}

func (s *localStorage) PutObject(ctx context.Context, key string, body io.Reader) error {
	_ = ctx
	key = cleanKey(key)
	if key == "" {
		return nil
	}
	path := filepath.Join(s.root, filepath.FromSlash(key))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, body)
	return err
}

func (s *localStorage) GetObject(ctx context.Context, key string) (io.ReadCloser, ObjectInfo, error) {
	_ = ctx
	path := filepath.Join(s.root, filepath.FromSlash(cleanKey(key)))
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ObjectInfo{}, errNotFound
		}
		return nil, ObjectInfo{}, err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, ObjectInfo{}, err
	}
	return f, ObjectInfo{
		Key:          cleanKey(key),
		Size:         st.Size(),
		ModTime:      st.ModTime().UTC(),
		ETag:         fmt.Sprintf("\"%x-%d\"", st.ModTime().UnixNano(), st.Size()),
		StorageClass: "STANDARD",
	}, nil
}

func (s *localStorage) HeadObject(ctx context.Context, key string) (ObjectInfo, error) {
	_ = ctx
	path := filepath.Join(s.root, filepath.FromSlash(cleanKey(key)))
	st, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return ObjectInfo{}, errNotFound
		}
		return ObjectInfo{}, err
	}
	return ObjectInfo{
		Key:          cleanKey(key),
		Size:         st.Size(),
		ModTime:      st.ModTime().UTC(),
		ETag:         fmt.Sprintf("\"%x-%d\"", st.ModTime().UnixNano(), st.Size()),
		StorageClass: "STANDARD",
	}, nil
}

func (s *localStorage) DeleteObject(ctx context.Context, key string) error {
	_ = ctx
	path := filepath.Join(s.root, filepath.FromSlash(cleanKey(key)))
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
