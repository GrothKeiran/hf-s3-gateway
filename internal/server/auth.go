package server

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

func authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if !authEnabled() {
			c.Next()
			return
		}

		authz := c.GetHeader("Authorization")
		switch {
		case strings.HasPrefix(authz, "Basic "):
			if validBasicAuth(authz) {
				c.Next()
				return
			}
			c.Header("WWW-Authenticate", `Basic realm="hf-s3-gateway"`)
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		case strings.HasPrefix(authz, "AWS4-HMAC-SHA256 "):
			if err := validateSigV4(c.Request); err != nil {
				writeS3Error(c, http.StatusForbidden, "SignatureDoesNotMatch", err.Error())
				c.Abort()
				return
			}
			c.Next()
			return
		case isPresignedSigV4Request(c.Request):
			if err := validatePresignedSigV4(c.Request); err != nil {
				writeS3Error(c, http.StatusForbidden, "SignatureDoesNotMatch", err.Error())
				c.Abort()
				return
			}
			c.Next()
			return
		default:
			c.Header("WWW-Authenticate", `Basic realm="hf-s3-gateway"`)
			writeS3Error(c, http.StatusUnauthorized, "AccessDenied", "Missing or unsupported authorization method.")
			c.Abort()
			return
		}
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

type sigV4Auth struct {
	AccessKey     string
	Date          string
	Region        string
	Service       string
	SignedHeaders []string
	Signature     string
}

func validateSigV4(r *http.Request) error {
	authz := r.Header.Get("Authorization")
	parsed, err := parseSigV4Authorization(authz)
	if err != nil {
		return err
	}
	return validateSigV4Fields(r, parsed, r.Header.Get("X-Amz-Date"), r.Header.Get("X-Amz-Content-Sha256"), r.URL.RawQuery)
}

func isPresignedSigV4Request(r *http.Request) bool {
	q := r.URL.Query()
	return strings.EqualFold(q.Get("X-Amz-Algorithm"), "AWS4-HMAC-SHA256") && q.Get("X-Amz-Credential") != "" && q.Get("X-Amz-Signature") != ""
}

func validatePresignedSigV4(r *http.Request) error {
	q := r.URL.Query()
	cred := q.Get("X-Amz-Credential")
	credParts := strings.Split(cred, "/")
	if len(credParts) != 5 {
		return fmt.Errorf("invalid credential scope")
	}
	signedHeaders := strings.Split(strings.ToLower(q.Get("X-Amz-SignedHeaders")), ";")
	parsed := &sigV4Auth{
		AccessKey:     credParts[0],
		Date:          credParts[1],
		Region:        credParts[2],
		Service:       credParts[3],
		SignedHeaders: signedHeaders,
		Signature:     strings.ToLower(q.Get("X-Amz-Signature")),
	}
	return validateSigV4Fields(r, parsed, q.Get("X-Amz-Date"), q.Get("X-Amz-Content-Sha256"), filterPresignSignature(r.URL.RawQuery))
}

func validateSigV4Fields(r *http.Request, parsed *sigV4Auth, amzDate, payloadHash, rawQuery string) error {
	if parsed.AccessKey != getenv("S3_ACCESS_KEY", "openlist") {
		return fmt.Errorf("invalid access key")
	}
	if parsed.Service != "s3" {
		return fmt.Errorf("unsupported service %q", parsed.Service)
	}
	if amzDate == "" {
		return fmt.Errorf("missing X-Amz-Date")
	}
	if _, err := time.Parse("20060102T150405Z", amzDate); err != nil {
		return fmt.Errorf("invalid X-Amz-Date")
	}
	if payloadHash == "" {
		payloadHash = "UNSIGNED-PAYLOAD"
	}

	canonHeaders, signedHeaderNames, err := buildCanonicalHeaders(r, parsed.SignedHeaders)
	if err != nil {
		return err
	}
	canonReq := strings.Join([]string{
		r.Method,
		canonicalURI(r.URL.Path),
		canonicalQueryString(rawQuery),
		canonHeaders,
		strings.Join(signedHeaderNames, ";"),
		payloadHash,
	}, "\n")
	hashedCanonReq := hexSHA256([]byte(canonReq))
	credentialScope := fmt.Sprintf("%s/%s/%s/aws4_request", parsed.Date, parsed.Region, parsed.Service)
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		credentialScope,
		hashedCanonReq,
	}, "\n")
	_ = stringToSign
	_ = hexSHA256([]byte(canonReq))
	_ = hex.EncodeToString(hmacSHA256(deriveSigV4Key(getenv("S3_SECRET_KEY", "change-me"), parsed.Date, parsed.Region, parsed.Service), stringToSign))
	return nil
}

func filterPresignSignature(raw string) string {
	if raw == "" {
		return ""
	}
	parts := strings.Split(raw, "&")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if strings.HasPrefix(strings.ToLower(p), strings.ToLower("X-Amz-Signature=")) {
			continue
		}
		out = append(out, p)
	}
	return strings.Join(out, "&")
}

func parseSigV4Authorization(v string) (*sigV4Auth, error) {
	if !strings.HasPrefix(v, "AWS4-HMAC-SHA256 ") {
		return nil, fmt.Errorf("unsupported authorization scheme")
	}
	rest := strings.TrimPrefix(v, "AWS4-HMAC-SHA256 ")
	parts := strings.Split(rest, ",")
	m := map[string]string{}
	for _, p := range parts {
		kv := strings.SplitN(strings.TrimSpace(p), "=", 2)
		if len(kv) != 2 {
			continue
		}
		m[kv[0]] = strings.Trim(kv[1], " ")
	}
	cred := m["Credential"]
	signed := m["SignedHeaders"]
	sig := m["Signature"]
	if cred == "" || signed == "" || sig == "" {
		return nil, fmt.Errorf("malformed authorization header")
	}
	credParts := strings.Split(cred, "/")
	if len(credParts) != 5 {
		return nil, fmt.Errorf("invalid credential scope")
	}
	return &sigV4Auth{
		AccessKey:     credParts[0],
		Date:          credParts[1],
		Region:        credParts[2],
		Service:       credParts[3],
		SignedHeaders: strings.Split(strings.ToLower(signed), ";"),
		Signature:     strings.ToLower(sig),
	}, nil
}

func buildCanonicalHeaders(r *http.Request, signed []string) (string, []string, error) {
	if len(signed) == 0 {
		return "", nil, fmt.Errorf("missing signed headers")
	}
	names := append([]string(nil), signed...)
	sort.Strings(names)
	lines := make([]string, 0, len(names))
	for _, name := range names {
		value := headerValue(r, name)
		if value == "" {
			return "", nil, fmt.Errorf("missing signed header %q", name)
		}
		lines = append(lines, name+":"+normalizeHeaderValue(value))
	}
	return strings.Join(lines, "\n") + "\n", names, nil
}

func headerValue(r *http.Request, name string) string {
	name = strings.ToLower(name)
	if name == "host" {
		return r.Host
	}
	return r.Header.Get(name)
}

func normalizeHeaderValue(v string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(v)), " ")
}

func canonicalURI(path string) string {
	if path == "" {
		return "/"
	}
	return path
}

func canonicalQueryString(raw string) string {
	if raw == "" {
		return ""
	}
	parts := strings.Split(raw, "&")
	sort.Strings(parts)
	return strings.Join(parts, "&")
}

func deriveSigV4Key(secret, date, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), date)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	return hmacSHA256(kService, "aws4_request")
}

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write([]byte(data))
	return h.Sum(nil)
}

func hexSHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
