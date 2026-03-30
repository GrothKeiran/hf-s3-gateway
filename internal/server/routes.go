package server

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
)

type listAllMyBucketsResult struct {
	XMLName xml.Name `xml:"ListAllMyBucketsResult"`
	Xmlns   string   `xml:"xmlns,attr,omitempty"`
	Owner   owner    `xml:"Owner"`
	Buckets buckets  `xml:"Buckets"`
}

type owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type buckets struct {
	Bucket []bucket `xml:"Bucket"`
}

type bucket struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

type listBucketResult struct {
	XMLName               xml.Name `xml:"ListBucketResult"`
	Xmlns                 string   `xml:"xmlns,attr,omitempty"`
	Name                  string   `xml:"Name"`
	Prefix                string   `xml:"Prefix"`
	MaxKeys               int      `xml:"MaxKeys"`
	IsTruncated           bool     `xml:"IsTruncated"`
	Contents              []obj    `xml:"Contents"`
	KeyCount              int      `xml:"KeyCount,omitempty"`
	ContinuationToken     string   `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string   `xml:"NextContinuationToken,omitempty"`
}

type obj struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
	Owner        *owner `xml:"Owner,omitempty"`
}

func RegisterRoutes(r *gin.Engine) {
	registerHealthRoutes(r)
	store := NewStorageFromEnv()
	r.Use(authMiddleware())
	r.GET("/", handleListBuckets)
	r.PUT("/:bucket", handleCreateBucket)
	r.HEAD("/:bucket", handleHeadBucket)
	r.GET("/:bucket", handleBucketOps(store))
	r.PUT("/:bucket/*key", handlePutObject(store))
	r.GET("/:bucket/*key", handleGetObject(store))
	r.HEAD("/:bucket/*key", handleHeadObject(store))
	r.DELETE("/:bucket/*key", handleDeleteObject(store))
}

func handleListBuckets(c *gin.Context) {
	b := getenv("HF_BUCKET", "default")
	writeXML(c, http.StatusOK, listAllMyBucketsResult{
		Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/",
		Owner: owner{ID: "hf-s3-gateway", DisplayName: "hf-s3-gateway"},
		Buckets: buckets{Bucket: []bucket{{Name: b, CreationDate: time.Now().UTC().Format(time.RFC3339)}}},
	})
}

func handleCreateBucket(c *gin.Context) { c.Status(http.StatusNotImplemented) }
func handleHeadBucket(c *gin.Context)   { c.Status(http.StatusOK) }

func handleBucketOps(store Storage) gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Query("list-type") == "2" || c.Query("prefix") != "" || c.Query("delimiter") != "" {
			handleListObjectsV2(store)(c)
			return
		}
		c.Status(http.StatusOK)
	}
}

func handleListObjectsV2(store Storage) gin.HandlerFunc {
	return func(c *gin.Context) {
		prefix := cleanKey(c.Query("prefix"))
		items, err := store.ListObjects(c.Request.Context(), prefix)
		if err != nil {
			handleStorageErr(c, err)
			return
		}
		out := make([]obj, 0, len(items))
		for _, item := range items {
			out = append(out, obj{
				Key:          item.Key,
				LastModified: item.ModTime.UTC().Format(time.RFC3339),
				ETag:         item.ETag,
				Size:         item.Size,
				StorageClass: valueOr(item.StorageClass, "STANDARD"),
			})
		}
		writeXML(c, http.StatusOK, listBucketResult{
			Xmlns:       "http://s3.amazonaws.com/doc/2006-03-01/",
			Name:        c.Param("bucket"),
			Prefix:      prefix,
			MaxKeys:     1000,
			IsTruncated: false,
			Contents:    out,
			KeyCount:    len(out),
		})
	}
}

func handlePutObject(store Storage) gin.HandlerFunc {
	return func(c *gin.Context) {
		key := cleanKey(c.Param("key"))
		if key == "" {
			c.Status(http.StatusBadRequest)
			return
		}
		if err := store.PutObject(c.Request.Context(), key, c.Request.Body); err != nil {
			handleStorageErr(c, err)
			return
		}
		c.Status(http.StatusOK)
	}
}

func handleGetObject(store Storage) gin.HandlerFunc {
	return func(c *gin.Context) {
		body, meta, err := store.GetObject(c.Request.Context(), cleanKey(c.Param("key")))
		if err != nil {
			handleStorageErr(c, err)
			return
		}
		defer body.Close()
		c.Header("Content-Length", fmt.Sprintf("%d", meta.Size))
		if !meta.ModTime.IsZero() {
			c.Header("Last-Modified", meta.ModTime.UTC().Format(http.TimeFormat))
		}
		if meta.ETag != "" {
			c.Header("ETag", meta.ETag)
		}
		c.Status(http.StatusOK)
		_, _ = io.Copy(c.Writer, body)
	}
}

func handleHeadObject(store Storage) gin.HandlerFunc {
	return func(c *gin.Context) {
		meta, err := store.HeadObject(c.Request.Context(), cleanKey(c.Param("key")))
		if err != nil {
			handleStorageErr(c, err)
			return
		}
		c.Header("Content-Length", fmt.Sprintf("%d", meta.Size))
		if !meta.ModTime.IsZero() {
			c.Header("Last-Modified", meta.ModTime.UTC().Format(http.TimeFormat))
		}
		if meta.ETag != "" {
			c.Header("ETag", meta.ETag)
		}
		c.Status(http.StatusOK)
	}
}

func handleDeleteObject(store Storage) gin.HandlerFunc {
	return func(c *gin.Context) {
		if err := store.DeleteObject(c.Request.Context(), cleanKey(c.Param("key"))); err != nil {
			handleStorageErr(c, err)
			return
		}
		c.Status(http.StatusNoContent)
	}
}

func cleanKey(key string) string {
	for len(key) > 0 && key[0] == '/' {
		key = key[1:]
	}
	return key
}

func writeXML(c *gin.Context, code int, v any) {
	c.Header("Content-Type", "application/xml")
	c.Status(code)
	out, _ := xml.MarshalIndent(v, "", "  ")
	_, _ = c.Writer.Write([]byte(xml.Header))
	_, _ = c.Writer.Write(out)
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func handleStorageErr(c *gin.Context, err error) {
	switch {
	case errors.Is(err, errNotFound):
		c.Status(http.StatusNotFound)
	case isHFNotImplemented(err):
		c.JSON(http.StatusNotImplemented, gin.H{"error": err.Error()})
	default:
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
	}
}

func valueOr(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}
