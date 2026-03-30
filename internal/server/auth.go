package server

import (
	"crypto/subtle"
	"encoding/base64"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

func authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if !authEnabled() {
			c.Next()
			return
		}

		if validBasicAuth(c.GetHeader("Authorization")) {
			c.Next()
			return
		}

		c.Header("WWW-Authenticate", `Basic realm="hf-s3-gateway"`)
		c.AbortWithStatus(http.StatusUnauthorized)
	}
}

func authEnabled() bool {
	return getenv("S3_ACCESS_KEY", "") != "" || getenv("S3_SECRET_KEY", "") != ""
}

func validBasicAuth(header string) bool {
	if !strings.HasPrefix(header, "Basic ") {
		return false
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(header, "Basic "))
	if err != nil {
		return false
	}
	parts := strings.SplitN(string(raw), ":", 2)
	if len(parts) != 2 {
		return false
	}
	userOK := subtle.ConstantTimeCompare([]byte(parts[0]), []byte(getenv("S3_ACCESS_KEY", "openlist"))) == 1
	passOK := subtle.ConstantTimeCompare([]byte(parts[1]), []byte(getenv("S3_SECRET_KEY", "change-me"))) == 1
	return userOK && passOK
}
