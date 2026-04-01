package server

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
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
	prefix = cleanKey(prefix)

	var outputs [][]byte
	var errs []error
	candidates := [][]string{
		{"buckets", "ls", s.cli.bucketURI(prefix)},
		{"buckets", "ls", s.cli.bucketURI("")},
		{"buckets", "list", s.cli.bucketURI(prefix)},
		{"buckets", "list", s.cli.bucketURI("")},
	}
	for _, args := range candidates {
		out, err := s.cli.run(ctx, args...)
		if err == nil {
			outputs = append(outputs, out)
			items := parseHFListOutput(out, prefix)
			if items != nil {
				sort.Slice(items, func(i, j int) bool { return items[i].Key < items[j].Key })
				return items, nil
			}
			continue
		}
		errs = append(errs, err)
	}
	if len(outputs) > 0 {
		return []ObjectInfo{}, nil
	}
	if len(errs) > 0 {
		return nil, errs[0]
	}
	return []ObjectInfo{}, nil
}

func parseHFListOutput(out []byte, prefix string) []ObjectInfo {
	text := strings.TrimSpace(string(out))
	if text == "" {
		return []ObjectInfo{}
	}
	if items, ok := parseHFListJSON(text, prefix); ok {
		return items
	}
	return parseHFListText(text, prefix)
}

func parseHFListJSON(text, prefix string) ([]ObjectInfo, bool) {
	var arr []map[string]any
	if err := json.Unmarshal([]byte(text), &arr); err == nil {
		return normalizeHFMaps(arr, prefix), true
	}
	var obj map[string]any
	if err := json.Unmarshal([]byte(text), &obj); err == nil {
		for _, key := range []string{"items", "objects", "entries", "files"} {
			if raw, ok := obj[key]; ok {
				if arrAny, ok := raw.([]any); ok {
					maps := make([]map[string]any, 0, len(arrAny))
					for _, v := range arrAny {
						if m, ok := v.(map[string]any); ok {
							maps = append(maps, m)
						}
					}
					return normalizeHFMaps(maps, prefix), true
				}
			}
		}
	}
	return nil, false
}

func normalizeHFMaps(items []map[string]any, prefix string) []ObjectInfo {
	out := make([]ObjectInfo, 0, len(items))
	for _, m := range items {
		key := firstString(m, "key", "path", "name")
		if key == "" {
			continue
		}
		key = strings.TrimPrefix(cleanKey(key), "/")
		if prefix != "" && !strings.HasPrefix(key, prefix) {
			continue
		}
		if isHFDirectory(m, key) {
			continue
		}
		out = append(out, ObjectInfo{
			Key:          key,
			Size:         firstInt64(m, "size", "bytes"),
			ModTime:      firstTime(m, "last_modified", "updated_at", "modified", "mtime"),
			ETag:         quoteETag(firstString(m, "etag", "e_tag")),
			StorageClass: "STANDARD",
		})
	}
	return out
}

func parseHFListText(text, prefix string) []ObjectInfo {
	out := make([]ObjectInfo, 0)
	s := bufio.NewScanner(strings.NewReader(text))
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		var key string
		var size int64
		for _, f := range fields {
			cf := strings.TrimSpace(strings.Trim(f, "\"'"))
			if strings.Contains(cf, "/") || strings.Contains(cf, ".") {
				key = strings.TrimPrefix(cf, "/")
			}
			if n, err := strconv.ParseInt(strings.TrimSuffix(cf, "B"), 10, 64); err == nil {
				size = n
			}
		}
		if key == "" {
			continue
		}
		key = cleanKey(key)
		if strings.HasSuffix(key, "/") {
			continue
		}
		if prefix != "" && !strings.HasPrefix(key, prefix) {
			continue
		}
		out = append(out, ObjectInfo{Key: key, Size: size, StorageClass: "STANDARD"})
	}
	return out
}

func firstString(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			switch t := v.(type) {
			case string:
				return t
			}
		}
	}
	return ""
}

func firstInt64(m map[string]any, keys ...string) int64 {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			switch t := v.(type) {
			case float64:
				return int64(t)
			case int64:
				return t
			case int:
				return int64(t)
			case string:
				if n, err := strconv.ParseInt(t, 10, 64); err == nil {
					return n
				}
			}
		}
	}
	return 0
}

func firstTime(m map[string]any, keys ...string) time.Time {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			if s, ok := v.(string); ok {
				for _, layout := range []string{time.RFC3339, time.RFC3339Nano, "2006-01-02 15:04:05", time.DateTime} {
					if ts, err := time.Parse(layout, s); err == nil {
						return ts.UTC()
					}
				}
			}
		}
	}
	return time.Time{}
}

func quoteETag(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	if strings.HasPrefix(s, "\"") && strings.HasSuffix(s, "\"") {
		return s
	}
	return fmt.Sprintf("\"%s\"", s)
}

func isHFDirectory(m map[string]any, key string) bool {
	if strings.HasSuffix(key, "/") {
		return true
	}
	for _, k := range []string{"type", "kind"} {
		if v, ok := m[k].(string); ok {
			v = strings.ToLower(v)
			if v == "dir" || v == "directory" || v == "folder" {
				return true
			}
		}
	}
	if v, ok := m["is_dir"].(bool); ok && v {
		return true
	}
	return false
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
