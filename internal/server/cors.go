package server

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

func corsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		origin := c.GetHeader("Origin")
		if allowed := allowedCORSOrigin(origin); allowed != "" {
			c.Header("Access-Control-Allow-Origin", allowed)
			if allowed != "*" {
				c.Header("Vary", "Origin")
			}
		}
		if hfSDKEnabled("CORS_ALLOW_CREDENTIALS", false) {
			c.Header("Access-Control-Allow-Credentials", "true")
		}
		c.Header("Access-Control-Allow-Methods", getenv("CORS_ALLOW_METHODS", "GET,HEAD,PUT,POST,DELETE,OPTIONS"))
		allowHeaders := strings.TrimSpace(c.GetHeader("Access-Control-Request-Headers"))
		if allowHeaders == "" {
			allowHeaders = getenv("CORS_ALLOW_HEADERS", "Authorization,Content-Type,Content-MD5,Content-Length,ETag,Range,Content-Range,X-Amz-Date,X-Amz-Content-Sha256,X-Amz-Security-Token,X-Amz-User-Agent,X-Amz-Copy-Source,X-Amz-Acl,X-Requested-With")
		}
		c.Header("Access-Control-Allow-Headers", allowHeaders)
		c.Header("Access-Control-Expose-Headers", getenv("CORS_EXPOSE_HEADERS", "ETag,Accept-Ranges,Content-Range,Content-Length,Content-Type,Last-Modified,Content-Disposition,Cache-Control,X-HF-S3-Get-Mode"))
		c.Header("Access-Control-Max-Age", getenv("CORS_MAX_AGE", "86400"))
		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	}
}

func allowedCORSOrigin(origin string) string {
	allowed := strings.TrimSpace(getenv("CORS_ALLOW_ORIGINS", "*"))
	if allowed == "" {
		return ""
	}
	if allowed == "*" {
		return "*"
	}
	if origin == "" {
		return ""
	}
	for _, item := range strings.Split(allowed, ",") {
		if strings.TrimSpace(item) == origin {
			return origin
		}
	}
	return ""
}
