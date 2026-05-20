package server

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
)

// PythonBridge manages a persistent Python subprocess for HF SDK operations,
// avoiding the overhead of spawning a new Python process per call.
type PythonBridge struct {
	cli     *hfCLI
	cmd     *exec.Cmd
	stdin   io.WriteCloser
	stdout  *bufio.Scanner
	mu      sync.Mutex
	nextID  atomic.Int64
	pending map[int64]chan bridgeResponse
	done    chan struct{}
	started bool
}

type bridgeRequest struct {
	ID     int64       `json:"id"`
	Method string      `json:"method"`
	Params interface{} `json:"params"`
}

type bridgeResponse struct {
	ID     int64           `json:"id"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  string          `json:"error,omitempty"`
}

// pythonBridgeScript is the embedded Python script that runs as a persistent
// subprocess, reading JSON-RPC requests from stdin and writing responses to stdout.
const pythonBridgeScript = `
import sys
import json
import base64
from huggingface_hub import HfApi, HfFileSystem

api = HfApi(token=None)
fs = HfFileSystem(token=None)

def handle_list_objects(params):
    bucket_id = params["bucket_id"]
    prefix = params.get("prefix", "")
    items = []
    for item in api.list_bucket_tree(bucket_id=bucket_id, prefix=prefix, recursive=True):
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
    return {"items": items}

def handle_get_meta(params):
    bucket_id = params["bucket_id"]
    key = params["key"]
    path = "buckets/" + bucket_id + "/" + key
    info = fs.info(path)
    last_modified = info.get("last_modified") or info.get("LastModified") or ""
    if hasattr(last_modified, "isoformat"):
        last_modified = last_modified.isoformat()
    return {
        "key": key,
        "size": int(info.get("size", 0) or 0),
        "mod_time": last_modified,
        "etag": info.get("etag", "") or "",
    }

def handle_delete_object(params):
    bucket_id = params["bucket_id"]
    key = params["key"]
    api.batch_bucket_files(bucket_id=bucket_id, delete=[key])
    return "ok"

def handle_put_object(params):
    bucket_id = params["bucket_id"]
    key = params["key"]
    data_b64 = params["data_b64"]
    data = base64.b64decode(data_b64)
    api.batch_bucket_files(bucket_id=bucket_id, add=[(data, key)])
    return "ok"

def handle_get_object(params):
    bucket_id = params["bucket_id"]
    key = params["key"]
    max_bytes = params.get("max_bytes", -1)
    path = "buckets/" + bucket_id + "/" + key
    info = fs.info(path)
    size = int(info.get("size", 0) or 0)
    if max_bytes >= 0 and size > max_bytes:
        raise RuntimeError("object too large for sdk get: {} > {}".format(size, max_bytes))
    with fs.open(path, "rb") as f:
        data = f.read()
    last_modified = info.get("last_modified") or info.get("LastModified") or ""
    if hasattr(last_modified, "isoformat"):
        last_modified = last_modified.isoformat()
    return {
        "key": key,
        "size": int(info.get("size", len(data)) or len(data)),
        "mod_time": last_modified,
        "etag": info.get("etag", "") or "",
        "data_b64": base64.b64encode(data).decode("ascii"),
    }

def handle_signed_url(params):
    bucket_id = params["bucket_id"]
    key = params["key"]
    path = "buckets/" + bucket_id + "/" + key
    try:
        url = fs.sign(path)
    except Exception as e:
        return {"url": "", "err": str(e)}
    return {"url": url or "", "err": ""}

handlers = {
    "list_objects": handle_list_objects,
    "get_meta": handle_get_meta,
    "delete_object": handle_delete_object,
    "put_object": handle_put_object,
    "get_object": handle_get_object,
    "signed_url": handle_signed_url,
}

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    rid = 0
    try:
        req = json.loads(line)
        rid = req.get("id", 0)
        method = req.get("method", "")
        params = req.get("params", {})
        handler = handlers.get(method)
        if handler is None:
            resp = {"id": rid, "error": "unknown method: " + method}
        else:
            result = handler(params)
            resp = {"id": rid, "result": result}
    except Exception as e:
        resp = {"id": rid, "error": str(e)}
    sys.stdout.write(json.dumps(resp, ensure_ascii=False) + "\n")
    sys.stdout.flush()
`

func newPythonBridge(cli *hfCLI) *PythonBridge {
	return &PythonBridge{
		cli:     cli,
		pending: make(map[int64]chan bridgeResponse),
		done:    make(chan struct{}),
	}
}

func (b *PythonBridge) start() error {
	if err := b.cli.ensureReady(); err != nil {
		return err
	}
	cmd := exec.Command("python3", "-c", pythonBridgeScript)
	cmd.Dir = b.cli.workDir
	cmd.Env = append(os.Environ(), b.cli.env()...)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("bridge stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return fmt.Errorf("bridge stdout pipe: %w", err)
	}
	var stderr strings.Builder
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		return fmt.Errorf("bridge start: %w", err)
	}

	b.cmd = cmd
	b.stdin = stdin
	b.stdout = bufio.NewScanner(stdout)
	b.stdout.Buffer(make([]byte, 0, 1024*1024), 50*1024*1024) // 50MB max line
	b.started = true

	// Reader goroutine: reads JSON responses and dispatches to pending callers.
	go func() {
		defer close(b.done)
		for b.stdout.Scan() {
			var resp bridgeResponse
			if err := json.Unmarshal(b.stdout.Bytes(), &resp); err != nil {
				log.Printf("bridge: invalid response: %v", err)
				continue
			}
			b.mu.Lock()
			ch, ok := b.pending[resp.ID]
			if ok {
				delete(b.pending, resp.ID)
			}
			b.mu.Unlock()
			if ok {
				ch <- resp
			}
		}
		if err := b.stdout.Err(); err != nil {
			log.Printf("bridge: scanner error: %v", err)
		}
		// Notify all pending requests that the bridge has exited.
		b.mu.Lock()
		for id, ch := range b.pending {
			ch <- bridgeResponse{ID: id, Error: "bridge process exited"}
			delete(b.pending, id)
		}
		b.mu.Unlock()
	}()

	log.Printf("python bridge started pid=%d", cmd.Process.Pid)
	return nil
}

func (b *PythonBridge) ensureStarted() error {
	if b.started && b.cmd != nil && b.cmd.Process != nil {
		if b.cmd.ProcessState != nil && b.cmd.ProcessState.Exited() {
			b.started = false
		}
	}
	if !b.started {
		return b.start()
	}
	return nil
}

func (b *PythonBridge) call(ctx context.Context, method string, params interface{}) (json.RawMessage, error) {
	if err := b.ensureStarted(); err != nil {
		return nil, err
	}

	id := b.nextID.Add(1)
	req := bridgeRequest{ID: id, Method: method, Params: params}
	data, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("bridge marshal: %w", err)
	}

	ch := make(chan bridgeResponse, 1)
	b.mu.Lock()
	b.pending[id] = ch
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		delete(b.pending, id)
		b.mu.Unlock()
	}()

	if _, err := b.stdin.Write(append(data, '\n')); err != nil {
		b.started = false
		return nil, fmt.Errorf("bridge write: %w", err)
	}

	select {
	case resp := <-ch:
		if resp.Error != "" {
			return nil, fmt.Errorf("bridge error: %s", resp.Error)
		}
		return resp.Result, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (b *PythonBridge) Close() {
	if b.stdin != nil {
		_ = b.stdin.Close()
	}
	if b.cmd != nil && b.cmd.Process != nil {
		_ = b.cmd.Process.Kill()
		_ = b.cmd.Wait()
	}
	b.started = false
}
