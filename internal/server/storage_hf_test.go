package server

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestParseHFListJSON(t *testing.T) {
	out := []byte(`[
	  {"path":"folder/a.txt","size":12,"etag":"abc","last_modified":"2026-03-31T06:00:00Z"},
	  {"path":"folder/sub/","type":"directory"},
	  {"path":"root.bin","size":7}
	]`)
	items := parseHFListOutput(out, "folder/")
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
	if items[0].Key != "folder/a.txt" {
		t.Fatalf("unexpected key: %s", items[0].Key)
	}
	if items[0].Size != 12 {
		t.Fatalf("unexpected size: %d", items[0].Size)
	}
	if items[0].ETag != "\"abc\"" {
		t.Fatalf("unexpected etag: %s", items[0].ETag)
	}
}

func TestParseHFListText(t *testing.T) {
	out := []byte("folder/a.txt 12\nfolder/sub/\nother.bin 7\n")
	items := parseHFListOutput(out, "folder/")
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
	if items[0].Key != "folder/a.txt" {
		t.Fatalf("unexpected key: %s", items[0].Key)
	}
}

func TestHFCLIStorageListObjectsWithFakeCLI(t *testing.T) {
	tmp := t.TempDir()
	fakeHF := filepath.Join(tmp, "hf")
	script := `#!/usr/bin/env bash
set -e
if [[ "$1" == "buckets" && ( "$2" == "ls" || "$2" == "list" ) ]]; then
  cat <<'JSON'
[
  {"path":"docs/readme.md","size":10,"etag":"e1","last_modified":"2026-03-31T06:00:00Z"},
  {"path":"docs/spec.txt","size":20,"etag":"e2","last_modified":"2026-03-31T07:00:00Z"},
  {"path":"images/","type":"directory"}
]
JSON
  exit 0
fi
exit 1
`
	if err := os.WriteFile(fakeHF, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	t.Setenv("HF_BIN", fakeHF)
	t.Setenv("HF_NAMESPACE", "ns")
	t.Setenv("HF_BUCKET", "bucket")
	t.Setenv("HF_WORK_DIR", filepath.Join(tmp, "work"))

	s := newHFPlaceholderStorage().(*hfCLIStorage)
	items, err := s.ListObjects(context.Background(), "docs/")
	if err != nil {
		t.Fatalf("ListObjects error: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(items))
	}
	if items[0].Key != "docs/readme.md" || items[1].Key != "docs/spec.txt" {
		t.Fatalf("unexpected keys: %+v", items)
	}
}
