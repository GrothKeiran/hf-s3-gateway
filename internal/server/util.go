package server

import "os"

func dataRoot() string {
	root := getenv("DATA_DIR", ".data")
	_ = os.MkdirAll(root, 0o755)
	return root
}
