package server

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
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
	r.GET("/", handleListBuckets)
	r.PUT("/:bucket", handleCreateBucket)
	r.HEAD("/:bucket", handleHeadBucket)
	r.GET("/:bucket", handleBucketOps)
	r.PUT("/:bucket/*key", handlePutObject)
	r.GET("/:bucket/*key", handleGetObject)
	r.HEAD("/:bucket/*key", handleHeadObject)
	r.DELETE("/:bucket/*key", handleDeleteObject)
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

func handleBucketOps(c *gin.Context) {
	if c.Query("list-type") == "2" || c.Query("prefix") != "" || c.Query("delimiter") != "" {
		handleListObjectsV2(c)
		return
	}
	c.Status(http.StatusOK)
}

func handleListObjectsV2(c *gin.Context) {
	root := dataRoot()
	prefix := strings.TrimPrefix(c.Query("prefix"), "/")
	items := make([]obj, 0)
	_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info == nil || info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return nil
		}
		key := filepath.ToSlash(rel)
		if prefix != "" && !strings.HasPrefix(key, prefix) {
			return nil
		}
		items = append(items, obj{
			Key:          key,
			LastModified: info.ModTime().UTC().Format(time.RFC3339),
			ETag:         fmt.Sprintf("\"%x-%d\"", info.ModTime().UnixNano(), info.Size()),
			Size:         info.Size(),
			StorageClass: "STANDARD",
		})
		return nil
	})
	sort.Slice(items, func(i, j int) bool { return items[i].Key < items[j].Key })
	writeXML(c, http.StatusOK, listBucketResult{
		Xmlns:       "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:        c.Param("bucket"),
		Prefix:      prefix,
		MaxKeys:     1000,
		IsTruncated: false,
		Contents:    items,
		KeyCount:    len(items),
	})
}

func handlePutObject(c *gin.Context) {
	key := cleanKey(c.Param("key"))
	if key == "" {
		c.Status(http.StatusBadRequest)
		return
	}
	path := filepath.Join(dataRoot(), filepath.FromSlash(key))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	f, err := os.Create(path)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	defer f.Close()
	if _, err := io.Copy(f, c.Request.Body); err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.Status(http.StatusOK)
}

func handleGetObject(c *gin.Context) {
	path := filepath.Join(dataRoot(), filepath.FromSlash(cleanKey(c.Param("key"))))
	if _, err := os.Stat(path); err != nil {
		c.Status(http.StatusNotFound)
		return
	}
	c.File(path)
}

func handleHeadObject(c *gin.Context) {
	path := filepath.Join(dataRoot(), filepath.FromSlash(cleanKey(c.Param("key"))))
	st, err := os.Stat(path)
	if err != nil {
		c.Status(http.StatusNotFound)
		return
	}
	c.Header("Content-Length", fmt.Sprintf("%d", st.Size()))
	c.Header("Last-Modified", st.ModTime().UTC().Format(http.TimeFormat))
	c.Status(http.StatusOK)
}

func handleDeleteObject(c *gin.Context) {
	path := filepath.Join(dataRoot(), filepath.FromSlash(cleanKey(c.Param("key"))))
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.Status(http.StatusNoContent)
}

func dataRoot() string {
	root := getenv("DATA_DIR", ".data")
	_ = os.MkdirAll(root, 0o755)
	return root
}

func cleanKey(key string) string {
	return strings.TrimPrefix(key, "/")
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
