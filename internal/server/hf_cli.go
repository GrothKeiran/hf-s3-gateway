package server

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

type hfCLI struct {
	bin       string
	namespace string
	bucket    string
	token     string
	workDir   string
}

func newHFCLIFromEnv() *hfCLI {
	return &hfCLI{
		bin:       getenv("HF_BIN", "hf"),
		namespace: getenv("HF_NAMESPACE", ""),
		bucket:    getenv("HF_BUCKET", ""),
		token:     getenv("HF_TOKEN", ""),
		workDir:   getenv("HF_WORK_DIR", filepath.Join(dataRoot(), ".hf-tmp")),
	}
}

func (c *hfCLI) enabled() bool {
	return c.namespace != "" && c.bucket != ""
}

func (c *hfCLI) bucketURI(key string) string {
	base := fmt.Sprintf("hf://buckets/%s/%s", c.namespace, c.bucket)
	key = cleanKey(key)
	if key == "" {
		return base
	}
	return base + "/" + key
}

func (c *hfCLI) ensureReady() error {
	if !c.enabled() {
		return fmt.Errorf("hf backend requires HF_NAMESPACE and HF_BUCKET")
	}
	if _, err := exec.LookPath(c.bin); err != nil {
		return fmt.Errorf("hf cli not found: %s", c.bin)
	}
	return os.MkdirAll(c.workDir, 0o755)
}

func (c *hfCLI) run(ctx context.Context, args ...string) ([]byte, error) {
	if err := c.ensureReady(); err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, c.bin, args...)
	cmd.Env = append(os.Environ(), c.env()...)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = err.Error()
		}
		return nil, fmt.Errorf("hf cli failed: %s", msg)
	}
	return stdout.Bytes(), nil
}

func (c *hfCLI) env() []string {
	if c.token == "" {
		return nil
	}
	return []string{"HF_TOKEN=" + c.token}
}
