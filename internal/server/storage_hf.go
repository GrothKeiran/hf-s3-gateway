package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type hfCLIStorage struct {
	cli    *hfCLI
	bridge *PythonBridge
}

type hfPyListResult struct {
	Items []ObjectInfo `json:"items"`
}

const (
	defaultHFSDKPutMaxBytes = 8 * 1024 * 1024
	defaultHFSDKGetMaxBytes = 8 * 1024 * 1024
)

func newHFPlaceholderStorage() Storage {
	cli := newHFCLIFromEnv()
	return &hfCLIStorage{
		cli:    cli,
		bridge: newPythonBridge(cli),
	}
}

func hfSDKEnabled(name string, fallback bool) bool {
	v := strings.TrimSpace(strings.ToLower(getenv(name, "")))
	if v == "" {
		return fallback
	}
	switch v {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}

func hfSDKMaxBytes(name string, fallback int64) int64 {
	v := strings.TrimSpace(getenv(name, ""))
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		return fallback
	}
	return n
}

func (s *hfCLIStorage) ListObjects(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	if err := s.cli.ensureReady(); err != nil {
		return nil, err
	}
	prefix = cleanKey(prefix)
	if hfSDKEnabled("HF_SDK_LIST", true) {
		if items, err := s.listObjectsViaPython(ctx, prefix); err == nil {
			sort.Slice(items, func(i, j int) bool { return items[i].Key < items[j].Key })
			return items, nil
		}
	}

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

var hfListTextLineRE = regexp.MustCompile(`^\s*(\d+)\s+(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})\s+(.+?)\s*$`)

func parseHFListText(text, prefix string) []ObjectInfo {
	out := make([]ObjectInfo, 0)
	s := bufio.NewScanner(strings.NewReader(text))
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" {
			continue
		}
		m := hfListTextLineRE.FindStringSubmatch(line)
		if len(m) == 5 {
			size, _ := strconv.ParseInt(m[1], 10, 64)
			modTime, _ := time.ParseInLocation("2006-01-02 15:04:05", m[2]+" "+m[3], time.UTC)
			key := cleanKey(strings.TrimSpace(m[4]))
			if key == "" || strings.HasSuffix(key, "/") {
				continue
			}
			if prefix != "" && !strings.HasPrefix(key, prefix) {
				continue
			}
			out = append(out, ObjectInfo{
				Key:          key,
				Size:         size,
				ModTime:      modTime.UTC(),
				StorageClass: "STANDARD",
			})
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		key := cleanKey(fields[len(fields)-1])
		if key == "" || strings.HasSuffix(key, "/") {
			continue
		}
		if prefix != "" && !strings.HasPrefix(key, prefix) {
			continue
		}
		var size int64
		if n, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimSpace(fields[0]), "B"), 10, 64); err == nil {
			size = n
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
	putMax := hfSDKMaxBytes("HF_SDK_PUT_MAX_BYTES", defaultHFSDKPutMaxBytes)
	if hfSDKEnabled("HF_SDK_PUT", true) && putMax > 0 {
		limited := &io.LimitedReader{R: body, N: putMax + 1}
		data, err := io.ReadAll(limited)
		if err != nil {
			return err
		}
		if int64(len(data)) <= putMax {
			log.Printf("hf put key=%s mode=sdk-bytes size=%d", cleanKey(key), len(data))
			if err := s.putObjectViaPythonBytes(ctx, key, data); err == nil {
				return nil
			}
			log.Printf("hf put key=%s mode=sdk-bytes fallback=cli-stream size=%d", cleanKey(key), len(data))
			return s.putObjectViaCLIStream(ctx, key, bytes.NewReader(data))
		}
		log.Printf("hf put key=%s mode=cli-stream size_gt=%d", cleanKey(key), putMax)
		return s.putObjectViaCLIStream(ctx, key, io.MultiReader(bytes.NewReader(data), body))
	}
	log.Printf("hf put key=%s mode=cli-stream no-sdk-threshold", cleanKey(key))
	return s.putObjectViaCLIStream(ctx, key, body)
}

func (s *hfCLIStorage) GetObject(ctx context.Context, key string) (io.ReadCloser, ObjectInfo, error) {
	if err := s.cli.ensureReady(); err != nil {
		return nil, ObjectInfo{}, err
	}
	getMax := hfSDKMaxBytes("HF_SDK_GET_MAX_BYTES", defaultHFSDKGetMaxBytes)
	if hfSDKEnabled("HF_SDK_GET", true) && getMax > 0 {
		if rc, meta, err := s.getObjectViaPython(ctx, key, getMax); err == nil {
			return rc, meta, nil
		}
	}
	return s.getObjectViaCLIStream(ctx, key)
}

func (s *hfCLIStorage) HeadObject(ctx context.Context, key string) (ObjectInfo, error) {
	key = cleanKey(key)
	// Fast path: metadata-only call via bridge (no file download)
	if meta, err := s.getObjectMetaViaPython(ctx, key); err == nil {
		return meta, nil
	}
	// Slow path: fall back to full GetObject (downloads entire file)
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
	if hfSDKEnabled("HF_SDK_DELETE", true) {
		if err := s.deleteObjectViaPython(ctx, key); err == nil {
			return nil
		}
	}
	_, err := s.cli.run(ctx, "buckets", "rm", "-y", s.cli.bucketURI(key))
	return err
}

// --- Bridge-based Python SDK methods ---

func (s *hfCLIStorage) listObjectsViaPython(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	params := map[string]string{
		"bucket_id": s.cli.namespace + "/" + s.cli.bucket,
		"prefix":    prefix,
	}
	result, err := s.bridge.call(ctx, "list_objects", params)
	if err != nil {
		return nil, err
	}
	var res hfPyListResult
	if err := json.Unmarshal(result, &res); err != nil {
		return nil, err
	}
	for i := range res.Items {
		res.Items[i].Key = cleanKey(res.Items[i].Key)
		if res.Items[i].StorageClass == "" {
			res.Items[i].StorageClass = "STANDARD"
		}
	}
	return res.Items, nil
}

func (s *hfCLIStorage) deleteObjectViaPython(ctx context.Context, key string) error {
	params := map[string]string{
		"bucket_id": s.cli.namespace + "/" + s.cli.bucket,
		"key":       cleanKey(key),
	}
	_, err := s.bridge.call(ctx, "delete_object", params)
	return err
}

func (s *hfCLIStorage) SignedGetURL(ctx context.Context, key string) (string, error) {
	if err := s.cli.ensureReady(); err != nil {
		return "", err
	}
	params := map[string]string{
		"bucket_id": s.cli.namespace + "/" + s.cli.bucket,
		"key":       cleanKey(key),
	}
	result, err := s.bridge.call(ctx, "signed_url", params)
	if err != nil {
		return "", err
	}
	var res struct {
		URL string `json:"url"`
		Err string `json:"err"`
	}
	if err := json.Unmarshal(result, &res); err != nil {
		return "", err
	}
	url := strings.TrimSpace(res.URL)
	if url == "" {
		if strings.TrimSpace(res.Err) != "" {
			return "", fmt.Errorf("sign failed: %s", strings.TrimSpace(res.Err))
		}
		return "", fmt.Errorf("empty signed url")
	}
	if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
		return "", fmt.Errorf("non-http signed url: %s", url)
	}
	log.Printf("hf get key=%s mode=redirect url=%s", cleanKey(key), url)
	return url, nil
}

func (s *hfCLIStorage) putObjectViaPythonBytes(ctx context.Context, key string, data []byte) error {
	params := map[string]string{
		"bucket_id": s.cli.namespace + "/" + s.cli.bucket,
		"key":       cleanKey(key),
		"data_b64":  base64.StdEncoding.EncodeToString(data),
	}
	_, err := s.bridge.call(ctx, "put_object", params)
	return err
}

func (s *hfCLIStorage) getObjectMetaViaPython(ctx context.Context, key string) (ObjectInfo, error) {
	params := map[string]string{
		"bucket_id": s.cli.namespace + "/" + s.cli.bucket,
		"key":       cleanKey(key),
	}
	result, err := s.bridge.call(ctx, "get_meta", params)
	if err != nil {
		return ObjectInfo{}, err
	}
	var res struct {
		Key     string `json:"key"`
		Size    int64  `json:"size"`
		ModTime string `json:"mod_time"`
		ETag    string `json:"etag"`
	}
	if err := json.Unmarshal(result, &res); err != nil {
		return ObjectInfo{}, err
	}
	return ObjectInfo{
		Key:          cleanKey(res.Key),
		Size:         res.Size,
		ModTime:      parseTimeString(res.ModTime),
		ETag:         quoteETag(res.ETag),
		StorageClass: "STANDARD",
	}, nil
}

func (s *hfCLIStorage) getObjectViaPython(ctx context.Context, key string, maxBytes int64) (io.ReadCloser, ObjectInfo, error) {
	params := map[string]interface{}{
		"bucket_id": s.cli.namespace + "/" + s.cli.bucket,
		"key":       cleanKey(key),
		"max_bytes": maxBytes,
	}
	result, err := s.bridge.call(ctx, "get_object", params)
	if err != nil {
		return nil, ObjectInfo{}, err
	}
	var res struct {
		Key     string `json:"key"`
		Size    int64  `json:"size"`
		ModTime string `json:"mod_time"`
		ETag    string `json:"etag"`
		DataB64 string `json:"data_b64"`
	}
	if err := json.Unmarshal(result, &res); err != nil {
		return nil, ObjectInfo{}, err
	}
	data, err := base64.StdEncoding.DecodeString(res.DataB64)
	if err != nil {
		return nil, ObjectInfo{}, err
	}
	meta := ObjectInfo{
		Key:          cleanKey(res.Key),
		Size:         res.Size,
		ModTime:      parseTimeString(res.ModTime),
		ETag:         quoteETag(res.ETag),
		StorageClass: "STANDARD",
	}
	if meta.Size == 0 {
		meta.Size = int64(len(data))
	}
	return io.NopCloser(bytes.NewReader(data)), meta, nil
}

// --- CLI stream methods (unchanged) ---

func (s *hfCLIStorage) putObjectViaCLIStream(ctx context.Context, key string, body io.Reader) error {
	if err := s.cli.ensureReady(); err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, s.cli.bin, "buckets", "cp", "-", s.cli.bucketURI(key))
	cmd.Dir = s.cli.workDir
	cmd.Env = append(os.Environ(), s.cli.env()...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	var stderr bytes.Buffer
	cmd.Stdout = io.Discard
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		return err
	}
	copyErrCh := make(chan error, 1)
	go func() {
		_, err := io.Copy(stdin, body)
		closeErr := stdin.Close()
		if err == nil {
			err = closeErr
		}
		copyErrCh <- err
	}()
	waitErr := cmd.Wait()
	copyErr := <-copyErrCh
	if copyErr != nil {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		if waitErr == nil {
			waitErr = copyErr
		}
	}
	if waitErr != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = waitErr.Error()
		}
		return fmt.Errorf("hf cli stream upload failed: %s", msg)
	}
	return nil
}

func (s *hfCLIStorage) getObjectViaCLIStream(ctx context.Context, key string) (io.ReadCloser, ObjectInfo, error) {
	meta, metaErr := s.getObjectMetaViaPython(ctx, key)

	cmd := exec.CommandContext(ctx, s.cli.bin, "buckets", "cp", s.cli.bucketURI(key), "-")
	cmd.Dir = s.cli.workDir
	cmd.Env = append(os.Environ(), s.cli.env()...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, ObjectInfo{}, err
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return nil, ObjectInfo{}, err
	}

	if metaErr != nil {
		meta = ObjectInfo{Key: cleanKey(key), StorageClass: "STANDARD"}
	}
	if meta.Key == "" {
		meta.Key = cleanKey(key)
		meta.StorageClass = "STANDARD"
	}

	return &cmdReadCloser{ReadCloser: stdout, cmd: cmd, stderr: &stderr}, meta, nil
}

func parseTimeString(s string) time.Time {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05", time.DateTime} {
		if ts, err := time.Parse(layout, s); err == nil {
			return ts.UTC()
		}
	}
	return time.Time{}
}

func isHFNotImplemented(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "hf backend not implemented yet") || strings.Contains(msg, "hf backend list not implemented yet")
}

type cmdReadCloser struct {
	io.ReadCloser
	cmd    *exec.Cmd
	stderr *bytes.Buffer
}

func (c *cmdReadCloser) Close() error {
	readErr := c.ReadCloser.Close()
	waitErr := c.cmd.Wait()
	if waitErr != nil {
		if ee, ok := waitErr.(*exec.ExitError); ok {
			if status, ok := ee.Sys().(syscall.WaitStatus); ok {
				if status.Signaled() && (status.Signal() == syscall.SIGPIPE || status.Signal() == syscall.SIGKILL) {
					if readErr != nil {
						return readErr
					}
					return nil
				}
			}
		}
		msg := strings.TrimSpace(c.stderr.String())
		if msg == "" {
			msg = waitErr.Error()
		}
		if readErr != nil {
			return fmt.Errorf("%v; hf cli stream download failed: %s", readErr, msg)
		}
		return fmt.Errorf("hf cli stream download failed: %s", msg)
	}
	return readErr
}
