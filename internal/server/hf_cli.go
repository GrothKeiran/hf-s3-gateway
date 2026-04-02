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
	for _, dir := range []string{
		c.workDir,
		filepath.Join(c.workDir, "home"),
		filepath.Join(c.workDir, ".cache"),
		filepath.Join(c.workDir, ".config"),
		filepath.Join(c.workDir, ".hf"),
		filepath.Join(c.workDir, ".xet"),
	} {
		if err := os.MkdirAll(dir, 0o777); err != nil {
			return err
		}
	}
	return nil
}

func (c *hfCLI) run(ctx context.Context, args ...string) ([]byte, error) {
	if err := c.ensureReady(); err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, c.bin, args...)
	cmd.Dir = c.workDir
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
	env := []string{
		"HOME=" + filepath.Join(c.workDir, "home"),
		"XDG_CACHE_HOME=" + filepath.Join(c.workDir, ".cache"),
		"XDG_CONFIG_HOME=" + filepath.Join(c.workDir, ".config"),
		"HF_HOME=" + filepath.Join(c.workDir, ".hf"),
		"HF_HUB_CACHE=" + filepath.Join(c.workDir, ".hf", "hub"),
		"HF_XET_CACHE=" + filepath.Join(c.workDir, ".xet"),
		"HF_XET_HIGH_PERFORMANCE=0",
	}
	if c.token != "" {
		env = append(env, "HF_TOKEN="+c.token)
	}
	return env
}
