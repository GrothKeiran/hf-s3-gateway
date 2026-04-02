package server

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type multipartStore struct {
	root string
}

type multipartUpload struct {
	UploadID  string                   `json:"upload_id"`
	Bucket    string                   `json:"bucket"`
	Key       string                   `json:"key"`
	CreatedAt time.Time                `json:"created_at"`
	Parts     map[int]multipartPartMeta `json:"parts"`
}

type multipartPartMeta struct {
	PartNumber int       `json:"part_number"`
	Path       string    `json:"path"`
	Size       int64     `json:"size"`
	ETag       string    `json:"etag"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type initiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr,omitempty"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

type completeMultipartUpload struct {
	Parts []completedPart `xml:"Part"`
}

type completedPart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type completeMultipartUploadResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr,omitempty"`
	Location string   `xml:"Location,omitempty"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

func newMultipartStore() *multipartStore {
	root := filepath.Join(dataRoot(), ".multipart")
	_ = os.MkdirAll(root, 0o755)
	return &multipartStore{root: root}
}

func (m *multipartStore) create(bucket, key string) (*multipartUpload, error) {
	uploadID := fmt.Sprintf("%d-%x", time.Now().UnixNano(), md5.Sum([]byte(bucket+":"+key+":"+strconv.FormatInt(time.Now().UnixNano(), 10))))
	dir := filepath.Join(m.root, uploadID)
	if err := os.MkdirAll(filepath.Join(dir, "parts"), 0o755); err != nil {
		return nil, err
	}
	u := &multipartUpload{
		UploadID:  uploadID,
		Bucket:    bucket,
		Key:       cleanKey(key),
		CreatedAt: time.Now().UTC(),
		Parts:     map[int]multipartPartMeta{},
	}
	if err := m.save(u); err != nil {
		return nil, err
	}
	return u, nil
}

func (m *multipartStore) load(uploadID string) (*multipartUpload, error) {
	b, err := os.ReadFile(filepath.Join(m.root, uploadID, "meta.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errNotFound
		}
		return nil, err
	}
	var u multipartUpload
	if err := json.Unmarshal(b, &u); err != nil {
		return nil, err
	}
	if u.Parts == nil {
		u.Parts = map[int]multipartPartMeta{}
	}
	return &u, nil
}

func (m *multipartStore) save(u *multipartUpload) error {
	dir := filepath.Join(m.root, u.UploadID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(u, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "meta.json"), b, 0o644)
}

func (m *multipartStore) abort(uploadID string) error {
	err := os.RemoveAll(filepath.Join(m.root, uploadID))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (m *multipartStore) putPart(ctx context.Context, uploadID string, partNumber int, body io.Reader) (multipartPartMeta, error) {
	_ = ctx
	u, err := m.load(uploadID)
	if err != nil {
		return multipartPartMeta{}, err
	}
	if partNumber < 1 || partNumber > 10000 {
		return multipartPartMeta{}, fmt.Errorf("invalid part number")
	}
	dir := filepath.Join(m.root, uploadID, "parts")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return multipartPartMeta{}, err
	}
	path := filepath.Join(dir, fmt.Sprintf("part-%05d.bin", partNumber))
	f, err := os.Create(path)
	if err != nil {
		return multipartPartMeta{}, err
	}
	defer f.Close()
	h := md5.New()
	n, err := io.Copy(io.MultiWriter(f, h), body)
	if err != nil {
		return multipartPartMeta{}, err
	}
	part := multipartPartMeta{
		PartNumber: partNumber,
		Path:       path,
		Size:       n,
		ETag:       fmt.Sprintf("\"%s\"", hex.EncodeToString(h.Sum(nil))),
		UpdatedAt:  time.Now().UTC(),
	}
	u.Parts[partNumber] = part
	if err := m.save(u); err != nil {
		return multipartPartMeta{}, err
	}
	return part, nil
}

func (m *multipartStore) complete(ctx context.Context, store Storage, uploadID string, requested []completedPart) (completeMultipartUploadResult, error) {
	u, err := m.load(uploadID)
	if err != nil {
		return completeMultipartUploadResult{}, err
	}
	if len(requested) == 0 {
		return completeMultipartUploadResult{}, fmt.Errorf("no parts provided")
	}
	sort.Slice(requested, func(i, j int) bool { return requested[i].PartNumber < requested[j].PartNumber })
	for i := 1; i < len(requested); i++ {
		if requested[i-1].PartNumber == requested[i].PartNumber {
			return completeMultipartUploadResult{}, fmt.Errorf("duplicate part numbers")
		}
	}
	tmpFile, err := os.CreateTemp(filepath.Join(m.root, uploadID), "assembled-*")
	if err != nil {
		return completeMultipartUploadResult{}, err
	}
	assembledPath := tmpFile.Name()
	defer os.Remove(assembledPath)
	defer tmpFile.Close()
	wholeHash := md5.New()
	mw := io.MultiWriter(tmpFile, wholeHash)
	for idx, part := range requested {
		meta, ok := u.Parts[part.PartNumber]
		if !ok {
			return completeMultipartUploadResult{}, errNotFound
		}
		if part.ETag != "" && strings.TrimSpace(part.ETag) != meta.ETag {
			return completeMultipartUploadResult{}, fmt.Errorf("etag mismatch for part %d", part.PartNumber)
		}
		if idx < len(requested)-1 && meta.Size < 5*1024*1024 {
			return completeMultipartUploadResult{}, fmt.Errorf("part %d too small", part.PartNumber)
		}
		f, err := os.Open(meta.Path)
		if err != nil {
			return completeMultipartUploadResult{}, err
		}
		if _, err := io.Copy(mw, f); err != nil {
			_ = f.Close()
			return completeMultipartUploadResult{}, err
		}
		_ = f.Close()
	}
	if _, err := tmpFile.Seek(0, 0); err != nil {
		return completeMultipartUploadResult{}, err
	}
	if err := store.PutObject(ctx, u.Key, tmpFile); err != nil {
		return completeMultipartUploadResult{}, err
	}
	etag := fmt.Sprintf("\"%s-%d\"", hex.EncodeToString(wholeHash.Sum(nil)), len(requested))
	_ = m.abort(uploadID)
	return completeMultipartUploadResult{
		Xmlns:    "http://s3.amazonaws.com/doc/2006-03-01/",
		Location: "/" + u.Bucket + "/" + u.Key,
		Bucket:   u.Bucket,
		Key:      u.Key,
		ETag:     etag,
	}, nil
}
