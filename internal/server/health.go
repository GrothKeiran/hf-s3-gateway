package server

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

func registerHealthRoutes(r *gin.Engine, store Storage) {
	r.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"ok":        true,
			"bucket":    getenv("HF_BUCKET", "default"),
			"namespace": getenv("HF_NAMESPACE", ""),
			"backend":   getenv("STORAGE_BACKEND", "local"),
		})
	})

	r.GET("/readyz", func(c *gin.Context) {
		if err := readinessCheck(c.Request.Context(), store); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"ok":      false,
				"backend": getenv("STORAGE_BACKEND", "local"),
				"error":   err.Error(),
			})
			return
		}
		c.JSON(http.StatusOK, gin.H{
			"ok":      true,
			"backend": getenv("STORAGE_BACKEND", "local"),
		})
	})
}

func readinessCheck(ctx context.Context, store Storage) error {
	ctx, cancel := context.WithTimeout(ctx, readinessTimeout())
	defer cancel()
	if hfStore, ok := store.(*hfCLIStorage); ok {
		if err := hfStore.cli.ensureReady(); err != nil {
			return err
		}
	}
	if hfSDKEnabled("READY_CHECK_STORAGE", false) {
		_, err := store.ListObjects(ctx, "")
		return err
	}
	return nil
}

func readinessTimeout() time.Duration {
	raw := getenv("READY_CHECK_TIMEOUT", "5s")
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 5 * time.Second
	}
	return d
}
