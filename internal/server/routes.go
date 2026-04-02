package server

import (
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
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
	XMLName               xml.Name       `xml:"ListBucketResult"`
	Xmlns                 string         `xml:"xmlns,attr,omitempty"`
	Name                  string         `xml:"Name"`
	Prefix                string         `xml:"Prefix"`
	Delimiter             string         `xml:"Delimiter,omitempty"`
	MaxKeys               int            `xml:"MaxKeys"`
	IsTruncated           bool           `xml:"IsTruncated"`
	Contents              []obj          `xml:"Contents"`
	CommonPrefixes        []commonPrefix `xml:"CommonPrefixes,omitempty"`
	KeyCount              int            `xml:"KeyCount,omitempty"`
	ContinuationToken     string         `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string         `xml:"NextContinuationToken,omitempty"`
}

type commonPrefix struct {
	Prefix string `xml:"Prefix"`
}

type obj struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
	Owner        *owner `xml:"Owner,omitempty"`
}

type s3Error struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource,omitempty"`
	RequestID string   `xml:"RequestId,omitempty"`
}

func RegisterRoutes(r *gin.Engine) {
	registerHealthRoutes(r)
	store := NewStorageFromEnv()
	mp := newMultipartStore()
	r.Use(authMiddleware())
	r.GET("/", handleListBuckets)
	r.PUT("/:bucket", handleCreateBucket)
	r.HEAD("/:bucket", handleHeadBucket)
	r.GET("/:bucket", handleBucketOps(store))
	r.POST("/:bucket/*key", handlePostObject(store, mp))
	r.PUT("/:bucket/*key", handlePutObject(store, mp))
	r.GET("/:bucket/*key", handleGetObject(store))
	r.HEAD("/:bucket/*key", handleHeadObject(store))
	r.DELETE("/:bucket/*key", handleDeleteObject(store, mp))
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
		delimiter := c.Query("delimiter")
		maxKeys := parseMaxKeys(c.Query("max-keys"), 1000)
		continuationToken := c.Query("continuation-token")

		items, err := store.ListObjects(c.Request.Context(), prefix)
		if err != nil {
			handleStorageErr(c, err)
			return
		}

		startAfter := decodeContinuationToken(continuationToken)
		filtered := items
		if startAfter != "" {
			filtered = make([]ObjectInfo, 0, len(items))
			for _, item := range items {
				if item.Key > startAfter {
					filtered = append(filtered, item)
				}
			}
		}

		contents := make([]obj, 0)
		commonSet := map[string]struct{}{}
		count := 0
		isTruncated := false
		nextToken := ""

		for _, item := range filtered {
			rest := strings.TrimPrefix(item.Key, prefix)
			if delimiter != "" {
				if idx := strings.Index(rest, delimiter); idx >= 0 {
					cp := prefix + rest[:idx+len(delimiter)]
					if _, ok := commonSet[cp]; !ok {
						if count >= maxKeys {
							isTruncated = true
							nextToken = encodeContinuationToken(item.Key)
							break
						}
						commonSet[cp] = struct{}{}
						count++
					}
					continue
				}
			}

			if count >= maxKeys {
				isTruncated = true
				nextToken = encodeContinuationToken(item.Key)
				break
			}
			contents = append(contents, obj{
				Key:          item.Key,
				LastModified: formatS3Time(item.ModTime),
				ETag:         item.ETag,
				Size:         item.Size,
				StorageClass: valueOr(item.StorageClass, "STANDARD"),
			})
			count++
		}

		commonPrefixes := make([]commonPrefix, 0, len(commonSet))
		for cp := range commonSet {
			commonPrefixes = append(commonPrefixes, commonPrefix{Prefix: cp})
		}
		sort.Slice(commonPrefixes, func(i, j int) bool { return commonPrefixes[i].Prefix < commonPrefixes[j].Prefix })

		writeXML(c, http.StatusOK, listBucketResult{
			Xmlns:                 "http://s3.amazonaws.com/doc/2006-03-01/",
			Name:                  c.Param("bucket"),
			Prefix:                prefix,
			Delimiter:             delimiter,
			MaxKeys:               maxKeys,
			IsTruncated:           isTruncated,
			Contents:              contents,
			CommonPrefixes:        commonPrefixes,
			KeyCount:              len(contents) + len(commonPrefixes),
			ContinuationToken:     continuationToken,
			NextContinuationToken: nextToken,
		})
	}
}

func handlePostObject(store Storage, mp *multipartStore) gin.HandlerFunc {
	return func(c *gin.Context) {
		key := cleanKey(c.Param("key"))
		if key == "" {
			writeS3Error(c, http.StatusBadRequest, "InvalidRequest", "Missing object key.")
			return
		}
		if _, ok := c.GetQuery("uploads"); ok {
			u, err := mp.create(c.Param("bucket"), key)
			if err != nil {
				handleStorageErr(c, err)
				return
			}
			writeXML(c, http.StatusOK, initiateMultipartUploadResult{
				Xmlns:    "http://s3.amazonaws.com/doc/2006-03-01/",
				Bucket:   c.Param("bucket"),
				Key:      key,
				UploadID: u.UploadID,
			})
			return
		}
		if uploadID := c.Query("uploadId"); uploadID != "" {
			var req completeMultipartUpload
			if err := xml.NewDecoder(c.Request.Body).Decode(&req); err != nil {
				writeS3Error(c, http.StatusBadRequest, "MalformedXML", "The XML you provided was not well-formed or did not validate.")
				return
			}
			res, err := mp.complete(c.Request.Context(), store, uploadID, req.Parts)
			if err != nil {
				handleMultipartErr(c, err)
				return
			}
			writeXML(c, http.StatusOK, res)
			return
		}
		c.Status(http.StatusNotFound)
	}
}

func handlePutObject(store Storage, mp *multipartStore) gin.HandlerFunc {
	return func(c *gin.Context) {
		key := cleanKey(c.Param("key"))
		if key == "" {
			c.Status(http.StatusBadRequest)
			return
		}
		if uploadID := c.Query("uploadId"); uploadID != "" {
			partNumber, err := strconv.Atoi(c.Query("partNumber"))
			if err != nil || partNumber <= 0 {
				writeS3Error(c, http.StatusBadRequest, "InvalidArgument", "Invalid partNumber.")
				return
			}
			part, err := mp.putPart(c.Request.Context(), uploadID, partNumber, c.Request.Body)
			if err != nil {
				handleMultipartErr(c, err)
				return
			}
			c.Header("ETag", part.ETag)
			c.Status(http.StatusOK)
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

func handleDeleteObject(store Storage, mp *multipartStore) gin.HandlerFunc {
	return func(c *gin.Context) {
		if uploadID := c.Query("uploadId"); uploadID != "" {
			if err := mp.abort(uploadID); err != nil {
				handleMultipartErr(c, err)
				return
			}
			c.Status(http.StatusNoContent)
			return
		}
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

func writeS3Error(c *gin.Context, status int, code, message string) {
	writeXML(c, status, s3Error{
		Code:      code,
		Message:   message,
		Resource:  c.Request.URL.Path,
		RequestID: "hf-s3-gateway",
	})
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
		writeS3Error(c, http.StatusNotFound, "NoSuchKey", "The specified key does not exist.")
	case isHFNotImplemented(err):
		writeS3Error(c, http.StatusNotImplemented, "NotImplemented", err.Error())
	default:
		writeS3Error(c, http.StatusInternalServerError, "InternalError", err.Error())
	}
}

func handleMultipartErr(c *gin.Context, err error) {
	if err == nil {
		return
	}
	msg := err.Error()
	switch {
	case errors.Is(err, errNotFound), strings.Contains(msg, "NoSuchUpload"):
		writeS3Error(c, http.StatusNotFound, "NoSuchUpload", "The specified multipart upload does not exist.")
	case strings.Contains(msg, "etag mismatch"):
		writeS3Error(c, http.StatusBadRequest, "InvalidPart", msg)
	case strings.Contains(msg, "duplicate part"):
		writeS3Error(c, http.StatusBadRequest, "InvalidPartOrder", msg)
	case strings.Contains(msg, "too small"):
		writeS3Error(c, http.StatusBadRequest, "EntityTooSmall", msg)
	case strings.Contains(msg, "Invalid part number"), strings.Contains(msg, "invalid part number"):
		writeS3Error(c, http.StatusBadRequest, "InvalidArgument", msg)
	default:
		writeS3Error(c, http.StatusInternalServerError, "InternalError", msg)
	}
}

func parseMaxKeys(raw string, fallback int) int {
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return fallback
	}
	if n > 1000 {
		return 1000
	}
	return n
}

func encodeContinuationToken(key string) string {
	if key == "" {
		return ""
	}
	return base64.StdEncoding.EncodeToString([]byte(key))
}

func decodeContinuationToken(tok string) string {
	if tok == "" {
		return ""
	}
	b, err := base64.StdEncoding.DecodeString(tok)
	if err != nil {
		return ""
	}
	return string(b)
}

func formatS3Time(t time.Time) string {
	if t.IsZero() {
		t = time.Unix(0, 0).UTC()
	}
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}

func valueOr(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}
