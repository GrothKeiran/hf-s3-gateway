package server

import (
	"bufio"
	"bytes"
	"context"
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
	cli *hfCLI
}

type hfPyListResult struct {
	Items []ObjectInfo `json:"items"`
}

const (
	defaultHFSDKPutMaxBytes = 8 * 1024 * 1024
	defaultHFSDKGetMaxBytes = 8 * 1024 * 1024
)

func newHFPlaceholderStorage() Storage {
	return &hfCLIStorage{cli: newHFCLIFromEnv()}
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

func (s *hfCLIStorage) listObjectsViaPython(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	code := `
import json
from huggingface_hub import HfApi

api = HfApi(token=None)
items = []
for item in api.list_bucket_tree(bucket_id="__BUCKET_ID__", prefix=__PREFIX_JSON__, recursive=True):
    if getattr(item, "type", "file") != "file":
        continue
    last_modified = getattr(item, "last_modified", None)
    if hasattr(last_modified, "isoformat"):
        last_modified = last_modified.isoformat()
    items.append({
        "Key": getattr(item, "path", ""),
        "Size": int(getattr(item, "size", 0) or 0),
        "ModTime": last_modified or "",
        "ETag": "",
        "StorageClass": "STANDARD",
    })
print(json.dumps({"items": items}, ensure_ascii=False))
`
	code = strings.ReplaceAll(code, "__BUCKET_ID__", s.cli.namespace+"/"+s.cli.bucket)
	code = strings.ReplaceAll(code, "__PREFIX_JSON__", strconv.Quote(prefix))
	out, err := s.runPython(ctx, code)
	if err != nil {
		return nil, err
	}
	var res hfPyListResult
	if err := json.Unmarshal(out, &res); err != nil {
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
	code := `
from huggingface_hub import HfApi
api = HfApi(token=None)
api.batch_bucket_files(bucket_id="__BUCKET_ID__", delete=[__KEY_JSON__])
print("ok")
`
	code = strings.ReplaceAll(code, "__BUCKET_ID__", s.cli.namespace+"/"+s.cli.bucket)
	code = strings.ReplaceAll(code, "__KEY_JSON__", strconv.Quote(cleanKey(key)))
	_, err := s.runPython(ctx, code)
	return err
}

func (s *hfCLIStorage) SignedGetURL(ctx context.Context, key string) (string, error) {
	if err := s.cli.ensureReady(); err != nil {
		return "", err
	}
	code := `
import json
from huggingface_hub import HfFileSystem
fs = HfFileSystem(token=None)
path = "buckets/__BUCKET_ID__/__KEY__"
url = ""
err = ""
try:
    url = fs.sign(path)
except Exception as e:
    err = str(e)
print(json.dumps({"url": url or "", "err": err}, ensure_ascii=False))
`
	code = strings.ReplaceAll(code, "__BUCKET_ID__", s.cli.namespace+"/"+s.cli.bucket)
	code = strings.ReplaceAll(code, "__KEY__", strings.ReplaceAll(cleanKey(key), "\\", "\\\\"))
	out, err := s.runPython(ctx, code)
	if err != nil {
		return "", err
	}
	var res struct {
		URL string `json:"url"`
		Err string `json:"err"`
	}
	if err := json.Unmarshal(out, &res); err != nil {
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

func (s *hfCLIStorage) getObjectMetaViaPython(ctx context.Context, key string) (ObjectInfo, error) {
	code := `
import json
from huggingface_hub import HfFileSystem
fs = HfFileSystem(token=None)
info = fs.info("buckets/__BUCKET_ID__/__KEY__")
last_modified = info.get("last_modified") or info.get("LastModified") or ""
if hasattr(last_modified, "isoformat"):
    last_modified = last_modified.isoformat()
print(json.dumps({
    "key": __KEY_JSON__,
    "size": int(info.get("size", 0) or 0),
    "mod_time": last_modified,
    "etag": info.get("etag", "") or "",
}, ensure_ascii=False))
`
	code = strings.ReplaceAll(code, "__BUCKET_ID__", s.cli.namespace+"/"+s.cli.bucket)
	code = strings.ReplaceAll(code, "__KEY__", strings.ReplaceAll(cleanKey(key), "\\", "\\\\"))
	code = strings.ReplaceAll(code, "__KEY_JSON__", strconv.Quote(cleanKey(key)))
	out, err := s.runPython(ctx, code)
	if err != nil {
		return ObjectInfo{}, err
	}
	var res struct {
		Key     string `json:"key"`
		Size    int64  `json:"size"`
		ModTime string `json:"mod_time"`
		ETag    string `json:"etag"`
	}
	if err := json.Unmarshal(out, &res); err != nil {
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

func (s *hfCLIStorage) putObjectViaPythonBytes(ctx context.Context, key string, data []byte) error {
	code := `
import base64
from huggingface_hub import HfApi
api = HfApi(token=None)
api.batch_bucket_files(bucket_id="__BUCKET_ID__", add=[(base64.b64decode(__DATA_B64__), __KEY_JSON__)])
print("ok")
`
	code = strings.ReplaceAll(code, "__BUCKET_ID__", s.cli.namespace+"/"+s.cli.bucket)
	code = strings.ReplaceAll(code, "__KEY_JSON__", strconv.Quote(cleanKey(key)))
	code = strings.ReplaceAll(code, "__DATA_B64__", strconv.Quote(encodeBase64(data)))
	_, err := s.runPython(ctx, code)
	return err
}

func (s *hfCLIStorage) getObjectViaPython(ctx context.Context, key string, maxBytes int64) (io.ReadCloser, ObjectInfo, error) {
	code := `
import base64
import json
from huggingface_hub import HfFileSystem

fs = HfFileSystem(token=None)
path = "buckets/__BUCKET_ID__/__KEY__"
info = fs.info(path)
size = int(info.get("size", 0) or 0)
if __MAX_BYTES__ >= 0 and size > __MAX_BYTES__:
    raise RuntimeError(f"object too large for sdk get path: {size} > __MAX_BYTES__")
with fs.open(path, "rb") as f:
    data = f.read()
last_modified = info.get("last_modified") or info.get("LastModified") or ""
if hasattr(last_modified, "isoformat"):
    last_modified = last_modified.isoformat()
print(json.dumps({
    "key": __KEY_JSON__,
    "size": int(info.get("size", len(data)) or len(data)),
    "mod_time": last_modified,
    "etag": info.get("etag", "") or "",
    "data_b64": base64.b64encode(data).decode("ascii"),
}, ensure_ascii=False))
`
	code = strings.ReplaceAll(code, "__BUCKET_ID__", s.cli.namespace+"/"+s.cli.bucket)
	code = strings.ReplaceAll(code, "__KEY__", strings.ReplaceAll(cleanKey(key), "\\", "\\\\"))
	code = strings.ReplaceAll(code, "__KEY_JSON__", strconv.Quote(cleanKey(key)))
	code = strings.ReplaceAll(code, "__MAX_BYTES__", strconv.FormatInt(maxBytes, 10))
	out, err := s.runPython(ctx, code)
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
	if err := json.Unmarshal(out, &res); err != nil {
		return nil, ObjectInfo{}, err
	}
	data, err := decodeBase64(res.DataB64)
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

func (s *hfCLIStorage) runPython(ctx context.Context, code string) ([]byte, error) {
	if err := s.cli.ensureReady(); err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, "python3", "-c", code)
	cmd.Dir = s.cli.workDir
	cmd.Env = append(os.Environ(), s.cli.env()...)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = err.Error()
		}
		return nil, fmt.Errorf("hf python failed: %s", msg)
	}
	return stdout.Bytes(), nil
}

func encodeBase64(b []byte) string {
	const table = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	if len(b) == 0 {
		return ""
	}
	out := make([]byte, 0, ((len(b)+2)/3)*4)
	for i := 0; i < len(b); i += 3 {
		var n uint32
		remain := len(b) - i
		n = uint32(b[i]) << 16
		if remain > 1 {
			n |= uint32(b[i+1]) << 8
		}
		if remain > 2 {
			n |= uint32(b[i+2])
		}
		out = append(out,
			table[(n>>18)&63],
			table[(n>>12)&63],
		)
		if remain > 1 {
			out = append(out, table[(n>>6)&63])
		} else {
			out = append(out, '=')
		}
		if remain > 2 {
			out = append(out, table[n&63])
		} else {
			out = append(out, '=')
		}
	}
	return string(out)
}

func decodeBase64(s string) ([]byte, error) {
	dec := make([]byte, 256)
	for i := range dec {
		dec[i] = 0xFF
	}
	const table = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	for i := 0; i < len(table); i++ {
		dec[table[i]] = byte(i)
	}
	clean := strings.TrimSpace(s)
	if clean == "" {
		return []byte{}, nil
	}
	if len(clean)%4 != 0 {
		return nil, fmt.Errorf("invalid base64 length")
	}
	out := make([]byte, 0, len(clean)/4*3)
	for i := 0; i < len(clean); i += 4 {
		c0, c1, c2, c3 := clean[i], clean[i+1], clean[i+2], clean[i+3]
		if dec[c0] == 0xFF || dec[c1] == 0xFF {
			return nil, fmt.Errorf("invalid base64 data")
		}
		n := uint32(dec[c0])<<18 | uint32(dec[c1])<<12
		pad := 0
		if c2 == '=' {
			pad = 2
		} else {
			if dec[c2] == 0xFF {
				return nil, fmt.Errorf("invalid base64 data")
			}
			n |= uint32(dec[c2]) << 6
		}
		if c3 == '=' {
			if pad == 0 {
				pad = 1
			}
		} else {
			if dec[c3] == 0xFF {
				return nil, fmt.Errorf("invalid base64 data")
			}
			n |= uint32(dec[c3])
		}
		out = append(out, byte(n>>16))
		if pad < 2 {
			out = append(out, byte((n>>8)&0xFF))
		}
		if pad == 0 {
			out = append(out, byte(n&0xFF))
		}
	}
	return out, nil
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
