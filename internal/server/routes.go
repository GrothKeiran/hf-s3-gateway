package server

import (
	"context"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path"
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
	EncodingType          string         `xml:"EncodingType,omitempty"`
	KeyCount              int            `xml:"KeyCount"`
	ContinuationToken     string         `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string         `xml:"NextContinuationToken,omitempty"`
	StartAfter            string         `xml:"StartAfter,omitempty"`
}

type listBucketV1Result struct {
	XMLName        xml.Name       `xml:"ListBucketResult"`
	Xmlns          string         `xml:"xmlns,attr,omitempty"`
	Name           string         `xml:"Name"`
	Prefix         string         `xml:"Prefix"`
	Marker         string         `xml:"Marker"`
	NextMarker     string         `xml:"NextMarker,omitempty"`
	MaxKeys        int            `xml:"MaxKeys"`
	Delimiter      string         `xml:"Delimiter,omitempty"`
	IsTruncated    bool           `xml:"IsTruncated"`
	Contents       []obj          `xml:"Contents"`
	CommonPrefixes []commonPrefix `xml:"CommonPrefixes,omitempty"`
	EncodingType   string         `xml:"EncodingType,omitempty"`
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

type bucketLocationResult struct {
	XMLName xml.Name `xml:"LocationConstraint"`
	Xmlns   string   `xml:"xmlns,attr,omitempty"`
	Value   string   `xml:",chardata"`
}

type versioningConfigurationResult struct {
	XMLName xml.Name `xml:"VersioningConfiguration"`
	Xmlns   string   `xml:"xmlns,attr,omitempty"`
}

type accessControlPolicyResult struct {
	XMLName xml.Name `xml:"AccessControlPolicy"`
	Xmlns   string   `xml:"xmlns,attr,omitempty"`
	Owner   owner    `xml:"Owner"`
}

type deleteObjectsRequest struct {
	XMLName xml.Name            `xml:"Delete"`
	Objects []deleteObjectEntry `xml:"Object"`
	Quiet   bool                `xml:"Quiet"`
}

type deleteObjectEntry struct {
	Key       string `xml:"Key"`
	VersionID string `xml:"VersionId,omitempty"`
}

type deleteObjectsResult struct {
	XMLName xml.Name             `xml:"DeleteResult"`
	Xmlns   string               `xml:"xmlns,attr,omitempty"`
	Deleted []deletedObjectEntry `xml:"Deleted,omitempty"`
	Errors  []deleteErrorEntry   `xml:"Error,omitempty"`
}

type deletedObjectEntry struct {
	Key       string `xml:"Key"`
	VersionID string `xml:"VersionId,omitempty"`
}

type deleteErrorEntry struct {
	Key       string `xml:"Key"`
	VersionID string `xml:"VersionId,omitempty"`
	Code      string `xml:"Code"`
	Message   string `xml:"Message"`
}

type copyObjectResult struct {
	XMLName      xml.Name `xml:"CopyObjectResult"`
	LastModified string   `xml:"LastModified"`
	ETag         string   `xml:"ETag"`
}

type listObjectsOptions struct {
	Prefix       string
	Delimiter    string
	MaxKeys      int
	StartAfter   string
	EncodingType string
	IncludeOwner bool
}

type listObjectsPage struct {
	Contents       []obj
	CommonPrefixes []commonPrefix
	KeyCount       int
	IsTruncated    bool
	NextCursor     string
}

type httpRangeSpec struct {
	Start int64
	End   int64
}

func RegisterRoutes(r *gin.Engine) {
	store := NewStorageFromEnv()
	registerHealthRoutes(r, store)
	mp := newMultipartStore()
	r.Use(corsMiddleware())
	r.Use(authMiddleware())
	r.GET("/", handleListBuckets)
	r.PUT("/:bucket", handleCreateBucket)
	r.HEAD("/:bucket", handleHeadBucket)
	r.GET("/:bucket", handleBucketOps(store, mp))
	r.POST("/:bucket", handleBucketPostOps(store))
	r.POST("/:bucket/*key", handlePostObject(store, mp))
	r.PUT("/:bucket/*key", handlePutObject(store, mp))
	r.GET("/:bucket/*key", handleGetObject(store, mp))
	r.HEAD("/:bucket/*key", handleHeadObject(store))
	r.DELETE("/:bucket/*key", handleDeleteObject(store, mp))
}

func handleListBuckets(c *gin.Context) {
	b := expectedBucket()
	writeXML(c, http.StatusOK, listAllMyBucketsResult{
		Xmlns:   "http://s3.amazonaws.com/doc/2006-03-01/",
		Owner:   owner{ID: "hf-s3-gateway", DisplayName: "hf-s3-gateway"},
		Buckets: buckets{Bucket: []bucket{{Name: b, CreationDate: time.Now().UTC().Format(time.RFC3339)}}},
	})
}

func handleCreateBucket(c *gin.Context) {
	if c.Param("bucket") != expectedBucket() {
		writeS3Error(c, http.StatusNotImplemented, "NotImplemented", "This gateway exposes one preconfigured bucket and cannot create arbitrary buckets.")
		return
	}
	c.Status(http.StatusOK)
}

func handleHeadBucket(c *gin.Context) {
	if !ensureBucket(c) {
		return
	}
	c.Status(http.StatusOK)
}

func handleBucketOps(store Storage, mp *multipartStore) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !ensureBucket(c) {
			return
		}
		switch {
		case hasQuery(c, "location"):
			writeXML(c, http.StatusOK, bucketLocationResult{
				Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/",
				Value: getenv("S3_REGION", "us-east-1"),
			})
		case hasQuery(c, "versioning"):
			writeXML(c, http.StatusOK, versioningConfigurationResult{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/"})
		case hasQuery(c, "acl"):
			writeXML(c, http.StatusOK, accessControlPolicyResult{
				Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/",
				Owner: owner{ID: "hf-s3-gateway", DisplayName: "hf-s3-gateway"},
			})
		case hasQuery(c, "uploads"):
			handleListMultipartUploads(mp)(c)
		case c.Query("list-type") == "2":
			handleListObjectsV2(store)(c)
		default:
			handleListObjectsV1(store)(c)
		}
	}
}

func handleBucketPostOps(store Storage) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !ensureBucket(c) {
			return
		}
		if hasQuery(c, "delete") {
			handleDeleteObjects(store)(c)
			return
		}
		writeS3Error(c, http.StatusNotImplemented, "NotImplemented", "Unsupported bucket subresource.")
	}
}

func handleListObjectsV1(store Storage) gin.HandlerFunc {
	return func(c *gin.Context) {
		prefix := cleanKey(c.Query("prefix"))
		marker := cleanKey(c.Query("marker"))
		encodingType := c.Query("encoding-type")
		items, err := listSortedObjects(c.Request.Context(), store, prefix)
		if err != nil {
			handleStorageErr(c, err)
			return
		}
		page := buildListObjectsPage(items, listObjectsOptions{
			Prefix:       prefix,
			Delimiter:    c.Query("delimiter"),
			MaxKeys:      parseMaxKeys(c.Query("max-keys"), 1000),
			StartAfter:   marker,
			EncodingType: encodingType,
		})
		writeXML(c, http.StatusOK, listBucketV1Result{
			Xmlns:          "http://s3.amazonaws.com/doc/2006-03-01/",
			Name:           c.Param("bucket"),
			Prefix:         encodeListField(prefix, encodingType),
			Marker:         encodeListField(marker, encodingType),
			NextMarker:     encodeListField(page.NextCursor, encodingType),
			MaxKeys:        parseMaxKeys(c.Query("max-keys"), 1000),
			Delimiter:      encodeListField(c.Query("delimiter"), encodingType),
			IsTruncated:    page.IsTruncated,
			Contents:       page.Contents,
			CommonPrefixes: page.CommonPrefixes,
			EncodingType:   normalizedEncodingType(encodingType),
		})
	}
}

func handleListObjectsV2(store Storage) gin.HandlerFunc {
	return func(c *gin.Context) {
		prefix := cleanKey(c.Query("prefix"))
		delimiter := c.Query("delimiter")
		maxKeys := parseMaxKeys(c.Query("max-keys"), 1000)
		continuationToken := c.Query("continuation-token")
		startAfter := cleanKey(c.Query("start-after"))
		encodingType := c.Query("encoding-type")
		if continuationToken != "" {
			startAfter = decodeContinuationToken(continuationToken)
		}

		items, err := listSortedObjects(c.Request.Context(), store, prefix)
		if err != nil {
			handleStorageErr(c, err)
			return
		}

		page := buildListObjectsPage(items, listObjectsOptions{
			Prefix:       prefix,
			Delimiter:    delimiter,
			MaxKeys:      maxKeys,
			StartAfter:   startAfter,
			EncodingType: encodingType,
			IncludeOwner: strings.EqualFold(c.Query("fetch-owner"), "true"),
		})

		nextToken := ""
		if page.IsTruncated {
			nextToken = encodeContinuationToken(page.NextCursor)
		}
		writeXML(c, http.StatusOK, listBucketResult{
			Xmlns:                 "http://s3.amazonaws.com/doc/2006-03-01/",
			Name:                  c.Param("bucket"),
			Prefix:                encodeListField(prefix, encodingType),
			Delimiter:             encodeListField(delimiter, encodingType),
			MaxKeys:               maxKeys,
			IsTruncated:           page.IsTruncated,
			Contents:              page.Contents,
			CommonPrefixes:        page.CommonPrefixes,
			EncodingType:          normalizedEncodingType(encodingType),
			KeyCount:              page.KeyCount,
			ContinuationToken:     continuationToken,
			NextContinuationToken: nextToken,
			StartAfter:            encodeListField(c.Query("start-after"), encodingType),
		})
	}
}

func handlePostObject(store Storage, mp *multipartStore) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !ensureBucket(c) {
			return
		}
		key := cleanKey(c.Param("key"))
		if key == "" {
			handleBucketPostOps(store)(c)
			return
		}
		if hasQuery(c, "uploads") {
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
		writeS3Error(c, http.StatusNotImplemented, "NotImplemented", "Unsupported object POST operation.")
	}
}

func handlePutObject(store Storage, mp *multipartStore) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !ensureBucket(c) {
			return
		}
		key := cleanKey(c.Param("key"))
		if key == "" {
			handleCreateBucket(c)
			return
		}
		if copySource := c.GetHeader("X-Amz-Copy-Source"); copySource != "" {
			handleCopyObject(c, store, key, copySource)
			return
		}
		if uploadID := c.Query("uploadId"); uploadID != "" {
			partNumber, err := strconv.Atoi(c.Query("partNumber"))
			if err != nil || partNumber <= 0 {
				writeS3Error(c, http.StatusBadRequest, "InvalidArgument", "Invalid partNumber.")
				return
			}
			part, err := mp.putPart(c.Request.Context(), uploadID, partNumber, objectRequestBody(c.Request))
			if err != nil {
				handleMultipartErr(c, err)
				return
			}
			c.Header("ETag", part.ETag)
			c.Status(http.StatusOK)
			return
		}
		if err := store.PutObject(c.Request.Context(), key, objectRequestBody(c.Request)); err != nil {
			handleStorageErr(c, err)
			return
		}
		if meta, err := store.HeadObject(c.Request.Context(), key); err == nil && meta.ETag != "" {
			c.Header("ETag", meta.ETag)
		}
		c.Status(http.StatusOK)
	}
}

func handleGetObject(store Storage, mp *multipartStore) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !ensureBucket(c) {
			return
		}
		key := cleanKey(c.Param("key"))
		if key == "" {
			handleBucketOps(store, mp)(c)
			return
		}
		if gatewayPresignRequested(c.Request) {
			expiresIn := int64EnvLocal("GATEWAY_PRESIGN_DEFAULT_EXPIRES", 3600)
			if raw := strings.TrimSpace(c.Query("expires")); raw != "" {
				if v, err := strconv.ParseInt(raw, 10, 64); err == nil && v > 0 {
					expiresIn = v
				}
			}
			url, expiresAt, err := gatewaySignedURL(c.Request, expiresIn)
			if err != nil {
				writeS3Error(c, http.StatusBadRequest, "InvalidArgument", err.Error())
				return
			}
			c.JSON(http.StatusOK, gin.H{"url": url, "expires": expiresAt})
			return
		}
		if uploadID := c.Query("uploadId"); uploadID != "" {
			handleListParts(mp, key, uploadID)(c)
			return
		}
		if redirectStore, ok := store.(RedirectURLStorage); ok && hfSDKEnabled("HF_REDIRECT_GET", true) && c.GetHeader("Range") == "" {
			if url, err := redirectStore.SignedGetURL(c.Request.Context(), key); err == nil && url != "" {
				c.Header("X-HF-S3-Get-Mode", "redirect")
				c.Redirect(http.StatusTemporaryRedirect, url)
				return
			} else if hfSDKEnabled("HF_REDIRECT_GET_REQUIRED", false) {
				msg := "HF signed redirect URL is unavailable"
				if err != nil {
					msg = err.Error()
				}
				writeS3Error(c, http.StatusBadGateway, "BadGateway", msg)
				return
			}
		}

		c.Header("X-HF-S3-Get-Mode", "proxy")
		body, meta, err := store.GetObject(c.Request.Context(), key)
		if err != nil {
			handleStorageErr(c, err)
			return
		}
		defer body.Close()

		rng, partial, err := parseRange(c.GetHeader("Range"), meta.Size)
		if err != nil {
			writeInvalidRange(c, meta.Size)
			return
		}
		length := meta.Size
		if partial {
			length = rng.End - rng.Start + 1
			if rng.Start > 0 {
				if _, err := io.CopyN(io.Discard, body, rng.Start); err != nil {
					writeInvalidRange(c, meta.Size)
					return
				}
			}
			c.Header("Content-Range", fmt.Sprintf("bytes %d-%d/%d", rng.Start, rng.End, meta.Size))
			setObjectHeaders(c, key, meta, length)
			c.Status(http.StatusPartialContent)
			_, _ = io.Copy(c.Writer, io.LimitReader(body, length))
			return
		}
		setObjectHeaders(c, key, meta, length)
		c.Status(http.StatusOK)
		_, _ = io.Copy(c.Writer, body)
	}
}

func handleHeadObject(store Storage) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !ensureBucket(c) {
			return
		}
		key := cleanKey(c.Param("key"))
		if key == "" {
			handleHeadBucket(c)
			return
		}
		meta, err := store.HeadObject(c.Request.Context(), key)
		if err != nil {
			handleStorageErr(c, err)
			return
		}
		rng, partial, err := parseRange(c.GetHeader("Range"), meta.Size)
		if err != nil {
			writeInvalidRange(c, meta.Size)
			return
		}
		length := meta.Size
		status := http.StatusOK
		if partial {
			length = rng.End - rng.Start + 1
			c.Header("Content-Range", fmt.Sprintf("bytes %d-%d/%d", rng.Start, rng.End, meta.Size))
			status = http.StatusPartialContent
		}
		setObjectHeaders(c, key, meta, length)
		c.Status(status)
	}
}

func handleDeleteObject(store Storage, mp *multipartStore) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !ensureBucket(c) {
			return
		}
		if uploadID := c.Query("uploadId"); uploadID != "" {
			if err := mp.abort(uploadID); err != nil {
				handleMultipartErr(c, err)
				return
			}
			c.Status(http.StatusNoContent)
			return
		}
		key := cleanKey(c.Param("key"))
		if key == "" {
			writeS3Error(c, http.StatusBadRequest, "InvalidArgument", "Missing object key.")
			return
		}
		if err := store.DeleteObject(c.Request.Context(), key); err != nil {
			handleStorageErr(c, err)
			return
		}
		c.Status(http.StatusNoContent)
	}
}

func handleDeleteObjects(store Storage) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req deleteObjectsRequest
		if err := xml.NewDecoder(c.Request.Body).Decode(&req); err != nil {
			writeS3Error(c, http.StatusBadRequest, "MalformedXML", "The XML you provided was not well-formed or did not validate.")
			return
		}
		res := deleteObjectsResult{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/"}
		for _, obj := range req.Objects {
			key := cleanKey(obj.Key)
			if key == "" {
				continue
			}
			if err := store.DeleteObject(c.Request.Context(), key); err != nil {
				if errors.Is(err, errNotFound) || isHFNotFound(err) {
					if !req.Quiet {
						res.Deleted = append(res.Deleted, deletedObjectEntry{Key: key, VersionID: obj.VersionID})
					}
					continue
				}
				res.Errors = append(res.Errors, deleteErrorEntry{Key: key, VersionID: obj.VersionID, Code: "InternalError", Message: err.Error()})
				continue
			}
			if !req.Quiet {
				res.Deleted = append(res.Deleted, deletedObjectEntry{Key: key, VersionID: obj.VersionID})
			}
		}
		writeXML(c, http.StatusOK, res)
	}
}

func handleCopyObject(c *gin.Context, store Storage, dstKey, copySource string) {
	srcBucket, srcKey, err := parseCopySource(copySource)
	if err != nil {
		writeS3Error(c, http.StatusBadRequest, "InvalidArgument", "Invalid X-Amz-Copy-Source.")
		return
	}
	if srcBucket != expectedBucket() {
		writeS3Error(c, http.StatusNotFound, "NoSuchBucket", "The specified source bucket does not exist.")
		return
	}
	body, srcMeta, err := store.GetObject(c.Request.Context(), srcKey)
	if err != nil {
		handleStorageErr(c, err)
		return
	}
	defer body.Close()
	if err := store.PutObject(c.Request.Context(), dstKey, body); err != nil {
		handleStorageErr(c, err)
		return
	}
	meta, err := store.HeadObject(c.Request.Context(), dstKey)
	if err != nil {
		meta = srcMeta
	}
	writeXML(c, http.StatusOK, copyObjectResult{
		LastModified: formatS3Time(valueOrTime(meta.ModTime, time.Now().UTC())),
		ETag:         meta.ETag,
	})
}

func parseCopySource(raw string) (string, string, error) {
	raw = strings.TrimSpace(raw)
	raw = strings.TrimPrefix(raw, "/")
	if i := strings.IndexByte(raw, '?'); i >= 0 {
		raw = raw[:i]
	}
	decoded, err := url.PathUnescape(raw)
	if err != nil {
		return "", "", err
	}
	parts := strings.SplitN(decoded, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("invalid copy source")
	}
	return parts[0], cleanKey(parts[1]), nil
}

func listSortedObjects(ctx context.Context, store Storage, prefix string) ([]ObjectInfo, error) {
	items, err := store.ListObjects(ctx, prefix)
	if err != nil {
		return nil, err
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Key < items[j].Key })
	return items, nil
}

func buildListObjectsPage(items []ObjectInfo, opts listObjectsOptions) listObjectsPage {
	page := listObjectsPage{Contents: make([]obj, 0), CommonPrefixes: make([]commonPrefix, 0)}
	if opts.MaxKeys == 0 {
		return page
	}
	seenPrefixes := map[string]struct{}{}
	lastCursor := ""
	for _, item := range items {
		if opts.Prefix != "" && !strings.HasPrefix(item.Key, opts.Prefix) {
			continue
		}
		if opts.StartAfter != "" && item.Key <= opts.StartAfter {
			continue
		}
		rest := strings.TrimPrefix(item.Key, opts.Prefix)
		if opts.Delimiter != "" {
			if idx := strings.Index(rest, opts.Delimiter); idx >= 0 {
				cp := opts.Prefix + rest[:idx+len(opts.Delimiter)]
				if opts.StartAfter != "" && cp <= opts.StartAfter {
					continue
				}
				if _, ok := seenPrefixes[cp]; ok {
					continue
				}
				if page.KeyCount >= opts.MaxKeys {
					page.IsTruncated = lastCursor != ""
					page.NextCursor = lastCursor
					break
				}
				seenPrefixes[cp] = struct{}{}
				page.CommonPrefixes = append(page.CommonPrefixes, commonPrefix{Prefix: encodeListField(cp, opts.EncodingType)})
				page.KeyCount++
				lastCursor = cp
				continue
			}
		}
		if page.KeyCount >= opts.MaxKeys {
			page.IsTruncated = lastCursor != ""
			page.NextCursor = lastCursor
			break
		}
		page.Contents = append(page.Contents, makeListObject(item, opts))
		page.KeyCount++
		lastCursor = item.Key
	}
	return page
}

func makeListObject(item ObjectInfo, opts listObjectsOptions) obj {
	out := obj{
		Key:          encodeListField(item.Key, opts.EncodingType),
		LastModified: formatS3Time(item.ModTime),
		ETag:         item.ETag,
		Size:         item.Size,
		StorageClass: valueOr(item.StorageClass, "STANDARD"),
	}
	if opts.IncludeOwner {
		out.Owner = &owner{ID: "hf-s3-gateway", DisplayName: "hf-s3-gateway"}
	}
	return out
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
	case errors.Is(err, errInvalidKey):
		writeS3Error(c, http.StatusBadRequest, "InvalidArgument", "Invalid object key.")
	case errors.Is(err, errNotFound), isHFNotFound(err):
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
	case errors.Is(err, errNoSuchUpload), strings.Contains(msg, "NoSuchUpload"):
		writeS3Error(c, http.StatusNotFound, "NoSuchUpload", "The specified multipart upload does not exist.")
	case errors.Is(err, errInvalidPart), strings.Contains(msg, "etag mismatch"), strings.Contains(msg, "missing part"):
		writeS3Error(c, http.StatusBadRequest, "InvalidPart", msg)
	case errors.Is(err, errInvalidPartOrd), strings.Contains(msg, "duplicate part"):
		writeS3Error(c, http.StatusBadRequest, "InvalidPartOrder", msg)
	case errors.Is(err, errEntityTooSmall), strings.Contains(msg, "too small"):
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
	if err != nil || n < 0 {
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

func valueOrTime(v, fallback time.Time) time.Time {
	if v.IsZero() {
		return fallback
	}
	return v
}

func expectedBucket() string {
	return getenv("HF_BUCKET", "default")
}

func ensureBucket(c *gin.Context) bool {
	if c.Param("bucket") == expectedBucket() {
		return true
	}
	writeS3Error(c, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist.")
	return false
}

func hasQuery(c *gin.Context, key string) bool {
	_, ok := c.GetQuery(key)
	return ok
}

func normalizedEncodingType(v string) string {
	if strings.EqualFold(v, "url") {
		return "url"
	}
	return ""
}

func encodeListField(v, encodingType string) string {
	if !strings.EqualFold(encodingType, "url") || v == "" {
		return v
	}
	return strings.ReplaceAll(url.QueryEscape(v), "+", "%20")
}

func setObjectHeaders(c *gin.Context, key string, meta ObjectInfo, length int64) {
	c.Header("Accept-Ranges", "bytes")
	c.Header("Content-Length", fmt.Sprintf("%d", length))
	c.Header("Content-Type", contentTypeForKey(key))
	if disposition := objectContentDisposition(key); disposition != "" {
		c.Header("Content-Disposition", disposition)
	}
	if cacheControl := strings.TrimSpace(getenv("OBJECT_CACHE_CONTROL", "")); cacheControl != "" {
		c.Header("Cache-Control", cacheControl)
	}
	if meta.ModTime.IsZero() {
		meta.ModTime = time.Unix(0, 0).UTC()
	}
	c.Header("Last-Modified", meta.ModTime.UTC().Format(http.TimeFormat))
	if meta.ETag != "" {
		c.Header("ETag", meta.ETag)
	}
}

func contentTypeForKey(key string) string {
	ext := strings.ToLower(path.Ext(key))
	common := map[string]string{
		".mkv":  "video/x-matroska",
		".m3u8": "application/vnd.apple.mpegurl",
		".ts":   "video/mp2t",
		".flv":  "video/x-flv",
		".webm": "video/webm",
		".mov":  "video/quicktime",
		".avi":  "video/x-msvideo",
		".mp3":  "audio/mpeg",
		".flac": "audio/flac",
		".m4a":  "audio/mp4",
		".wasm": "application/wasm",
		".doc":  "application/msword",
		".xls":  "application/vnd.ms-excel",
		".ppt":  "application/vnd.ms-powerpoint",
		".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
		".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
		".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
	}
	if ct := common[ext]; ct != "" {
		return ct
	}
	if ct := mime.TypeByExtension(ext); ct != "" {
		return ct
	}
	return "application/octet-stream"
}

func objectContentDisposition(key string) string {
	mode := strings.TrimSpace(getenv("OBJECT_CONTENT_DISPOSITION", "inline"))
	if mode == "" || strings.EqualFold(mode, "none") {
		return ""
	}
	filename := path.Base(key)
	if filename == "." || filename == "/" || filename == "" {
		return mode
	}
	return fmt.Sprintf("%s; filename*=UTF-8''%s", mode, strings.ReplaceAll(url.PathEscape(filename), "+", "%20"))
}

func parseRange(header string, size int64) (httpRangeSpec, bool, error) {
	if header == "" {
		return httpRangeSpec{}, false, nil
	}
	if size < 0 || !strings.HasPrefix(header, "bytes=") || strings.Contains(header, ",") {
		return httpRangeSpec{}, false, fmt.Errorf("invalid range")
	}
	spec := strings.TrimPrefix(header, "bytes=")
	parts := strings.SplitN(spec, "-", 2)
	if len(parts) != 2 {
		return httpRangeSpec{}, false, fmt.Errorf("invalid range")
	}
	if size == 0 {
		return httpRangeSpec{}, false, fmt.Errorf("invalid range")
	}
	var start, end int64
	if parts[0] == "" {
		suffix, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil || suffix <= 0 {
			return httpRangeSpec{}, false, fmt.Errorf("invalid range")
		}
		if suffix > size {
			suffix = size
		}
		start = size - suffix
		end = size - 1
	} else {
		var err error
		start, err = strconv.ParseInt(parts[0], 10, 64)
		if err != nil || start < 0 {
			return httpRangeSpec{}, false, fmt.Errorf("invalid range")
		}
		if parts[1] == "" {
			end = size - 1
		} else {
			end, err = strconv.ParseInt(parts[1], 10, 64)
			if err != nil || end < 0 {
				return httpRangeSpec{}, false, fmt.Errorf("invalid range")
			}
			if end >= size {
				end = size - 1
			}
		}
	}
	if start >= size || start > end {
		return httpRangeSpec{}, false, fmt.Errorf("invalid range")
	}
	return httpRangeSpec{Start: start, End: end}, true, nil
}

func writeInvalidRange(c *gin.Context, size int64) {
	c.Header("Content-Range", fmt.Sprintf("bytes */%d", size))
	writeS3Error(c, http.StatusRequestedRangeNotSatisfiable, "InvalidRange", "The requested range is not satisfiable.")
}
