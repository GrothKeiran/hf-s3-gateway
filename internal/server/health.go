package server

import "github.com/gin-gonic/gin"

func registerHealthRoutes(r *gin.Engine) {
	r.GET("/healthz", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"ok": true,
			"bucket": getenv("HF_BUCKET", "default"),
			"namespace": getenv("HF_NAMESPACE", ""),
			"backend": getenv("STORAGE_BACKEND", "local"),
		})
	})
}
