package s3

import (
	"crypto/md5"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// fakeS3 is a minimal, in-memory S3-compatible REST server: just enough
// of the wire protocol (GET with Range, conditional PUT via
// If-None-Match, HEAD, DELETE, ListObjectsV2) to exercise the real
// aws-sdk-go-v2 S3 client end to end. It does not verify SigV4 request
// signing — that validates AWS's server, not our client, and is out of
// scope for testing our Backend implementation.
type fakeS3 struct {
	mu      sync.Mutex
	objects map[string][]byte
	bucket  string
}

func newFakeS3(bucket string) *httptest.Server {
	f := &fakeS3{objects: map[string][]byte{}, bucket: bucket}
	return httptest.NewServer(http.HandlerFunc(f.handle))
}

type xmlError struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

func writeS3Error(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_ = xml.NewEncoder(w).Encode(xmlError{Code: code, Message: msg})
}

func (f *fakeS3) handle(w http.ResponseWriter, r *http.Request) {
	prefix := "/" + f.bucket
	if !strings.HasPrefix(r.URL.Path, prefix) {
		http.NotFound(w, r)
		return
	}
	rest := strings.TrimPrefix(r.URL.Path, prefix)
	rest = strings.TrimPrefix(rest, "/")

	if rest == "" && r.URL.Query().Get("list-type") == "2" {
		f.list(w, r)
		return
	}
	if rest == "" {
		writeS3Error(w, 400, "InvalidRequest", "expected an object key or list-type=2")
		return
	}

	switch r.Method {
	case http.MethodPut:
		f.put(w, r, rest)
	case http.MethodGet:
		f.get(w, r, rest)
	case http.MethodHead:
		f.head(w, r, rest)
	case http.MethodDelete:
		f.delete(w, r, rest)
	default:
		writeS3Error(w, 405, "MethodNotAllowed", r.Method)
	}
}

func (f *fakeS3) put(w http.ResponseWriter, r *http.Request, key string) {
	body := make([]byte, r.ContentLength)
	if r.ContentLength > 0 {
		if _, err := io.ReadFull(r.Body, body); err != nil {
			writeS3Error(w, 500, "InternalError", err.Error())
			return
		}
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if r.Header.Get("If-None-Match") == "*" {
		if _, exists := f.objects[key]; exists {
			writeS3Error(w, 412, "PreconditionFailed", "At least one of the pre-conditions you specified did not hold")
			return
		}
	}
	f.objects[key] = body
	sum := md5.Sum(body)
	w.Header().Set("ETag", fmt.Sprintf(`"%x"`, sum))
	w.WriteHeader(200)
}

func (f *fakeS3) get(w http.ResponseWriter, r *http.Request, key string) {
	f.mu.Lock()
	data, ok := f.objects[key]
	f.mu.Unlock()
	if !ok {
		writeS3Error(w, 404, "NoSuchKey", "The specified key does not exist.")
		return
	}

	start, end := 0, len(data)-1
	if rng := r.Header.Get("Range"); rng != "" {
		var s, e int
		rng = strings.TrimPrefix(rng, "bytes=")
		parts := strings.SplitN(rng, "-", 2)
		s, _ = strconv.Atoi(parts[0])
		if len(parts) > 1 && parts[1] != "" {
			e, _ = strconv.Atoi(parts[1])
		} else {
			e = len(data) - 1
		}
		start, end = s, e
		if end >= len(data) {
			end = len(data) - 1
		}
		if start > end || start >= len(data) {
			writeS3Error(w, 416, "InvalidRange", "range not satisfiable")
			return
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		w.WriteHeader(206)
		_, _ = w.Write(data[start : end+1])
		return
	}
	w.WriteHeader(200)
	_, _ = w.Write(data)
}

func (f *fakeS3) head(w http.ResponseWriter, r *http.Request, key string) {
	f.mu.Lock()
	data, ok := f.objects[key]
	f.mu.Unlock()
	if !ok {
		w.WriteHeader(404) // HEAD: no body, per HTTP semantics
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(200)
}

func (f *fakeS3) delete(w http.ResponseWriter, r *http.Request, key string) {
	f.mu.Lock()
	delete(f.objects, key)
	f.mu.Unlock()
	w.WriteHeader(204)
}

type listContents struct {
	Key          string `xml:"Key"`
	Size         int64  `xml:"Size"`
	LastModified string `xml:"LastModified"`
}

type listResult struct {
	XMLName     xml.Name       `xml:"ListBucketResult"`
	Name        string         `xml:"Name"`
	Prefix      string         `xml:"Prefix"`
	KeyCount    int            `xml:"KeyCount"`
	MaxKeys     int            `xml:"MaxKeys"`
	IsTruncated bool           `xml:"IsTruncated"`
	Contents    []listContents `xml:"Contents"`
}

func (f *fakeS3) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	prefix := q.Get("prefix")
	startAfter := q.Get("start-after")
	maxKeys := 1000
	if mk := q.Get("max-keys"); mk != "" {
		if n, err := strconv.Atoi(mk); err == nil && n > 0 {
			maxKeys = n
		}
	}

	f.mu.Lock()
	var keys []string
	for k := range f.objects {
		if strings.HasPrefix(k, prefix) && k > startAfter {
			keys = append(keys, k)
		}
	}
	f.mu.Unlock()
	sort.Strings(keys)

	truncated := false
	if len(keys) > maxKeys {
		keys = keys[:maxKeys]
		truncated = true
	}

	res := listResult{Name: f.bucket, Prefix: prefix, MaxKeys: maxKeys, KeyCount: len(keys), IsTruncated: truncated}
	f.mu.Lock()
	for _, k := range keys {
		res.Contents = append(res.Contents, listContents{Key: k, Size: int64(len(f.objects[k])), LastModified: "2024-01-01T00:00:00.000Z"})
	}
	f.mu.Unlock()

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(200)
	_, _ = w.Write([]byte(xml.Header))
	_ = xml.NewEncoder(w).Encode(res)
}
