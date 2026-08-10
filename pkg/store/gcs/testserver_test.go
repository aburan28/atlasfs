package gcs

import (
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// fakeGCS is a minimal, in-memory server for the GCS JSON API: just
// enough of the wire protocol (multipart insert with ifGenerationMatch,
// media GET with Range, metadata GET, prefix/pageToken listing, DELETE)
// to exercise the real cloud.google.com/go/storage client end to end. It
// does not verify OAuth bearer tokens — the client is configured with
// option.WithoutAuthentication in tests, matching how pkg/store/s3's fake
// server never validates SigV4 signing: that would test GCS's server, not
// our Backend.
type fakeGCS struct {
	mu      sync.Mutex
	bucket  string
	objects map[string]*gcsObject
	genSeq  int64
}

type gcsObject struct {
	data       []byte
	generation int64
	updated    time.Time
}

func newFakeGCS(bucket string) *httptest.Server {
	f := &fakeGCS{bucket: bucket, objects: map[string]*gcsObject{}}
	return httptest.NewServer(http.HandlerFunc(f.handle))
}

type gcsErrorBody struct {
	Error gcsErrorDetail `json:"error"`
}

type gcsErrorDetail struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func writeGCSError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(gcsErrorBody{Error: gcsErrorDetail{Code: status, Message: msg}})
}

func (f *fakeGCS) handle(w http.ResponseWriter, r *http.Request) {
	uploadPrefix := "/upload/storage/v1/b/" + f.bucket + "/o"
	objPrefix := "/b/" + f.bucket + "/o/"
	listPath := "/b/" + f.bucket + "/o"

	switch {
	case r.Method == http.MethodPost && r.URL.Path == uploadPrefix:
		f.insert(w, r)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, objPrefix):
		f.get(w, r, strings.TrimPrefix(r.URL.Path, objPrefix))
	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, objPrefix):
		f.delete(w, r, strings.TrimPrefix(r.URL.Path, objPrefix))
	case r.Method == http.MethodGet && r.URL.Path == listPath:
		f.list(w, r)
	default:
		writeGCSError(w, 404, "not found: "+r.Method+" "+r.URL.Path)
	}
}

// objectResource is the subset of the GCS Object resource
// (https://cloud.google.com/storage/docs/json_api/v1/objects#resource)
// the Go client actually reads back out of responses.
type objectResource struct {
	Name       string `json:"name"`
	Bucket     string `json:"bucket"`
	Generation string `json:"generation"`
	Size       string `json:"size"`
	Updated    string `json:"updated"`
}

func (f *fakeGCS) resource(name string, o *gcsObject) objectResource {
	return objectResource{
		Name:       name,
		Bucket:     f.bucket,
		Generation: strconv.FormatInt(o.generation, 10),
		Size:       strconv.Itoa(len(o.data)),
		Updated:    o.updated.UTC().Format(time.RFC3339Nano),
	}
}

func (f *fakeGCS) insert(w http.ResponseWriter, r *http.Request) {
	name, err := url.QueryUnescape(r.URL.Query().Get("name"))
	if err != nil || name == "" {
		writeGCSError(w, 400, "missing or invalid name")
		return
	}

	_, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		writeGCSError(w, 400, "bad content-type: "+err.Error())
		return
	}
	mr := multipart.NewReader(r.Body, params["boundary"])
	// Part 1: JSON object metadata (bucket/name echoed by the client;
	// unused here — name comes from the query param, which is what a
	// real GCS server also treats as authoritative for the upload URL).
	if _, err := mr.NextPart(); err != nil {
		writeGCSError(w, 400, "missing metadata part: "+err.Error())
		return
	}
	dataPart, err := mr.NextPart()
	if err != nil {
		writeGCSError(w, 400, "missing media part: "+err.Error())
		return
	}
	data, err := io.ReadAll(dataPart)
	if err != nil {
		writeGCSError(w, 500, err.Error())
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if r.URL.Query().Get("ifGenerationMatch") == "0" {
		if _, exists := f.objects[name]; exists {
			writeGCSError(w, 412, "Precondition Failed")
			return
		}
	}
	f.genSeq++
	obj := &gcsObject{data: data, generation: f.genSeq, updated: time.Now()}
	f.objects[name] = obj

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	_ = json.NewEncoder(w).Encode(f.resource(name, obj))
}

func (f *fakeGCS) get(w http.ResponseWriter, r *http.Request, escapedName string) {
	name, err := url.PathUnescape(escapedName)
	if err != nil {
		writeGCSError(w, 400, "bad object name")
		return
	}

	f.mu.Lock()
	obj, ok := f.objects[name]
	f.mu.Unlock()
	if !ok {
		writeGCSError(w, 404, "not found")
		return
	}

	if r.URL.Query().Get("alt") != "media" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_ = json.NewEncoder(w).Encode(f.resource(name, obj))
		return
	}

	data := obj.data
	if rng := r.Header.Get("Range"); rng != "" {
		start, end, ok := parseRange(rng, len(data))
		if !ok {
			writeGCSError(w, 416, "range not satisfiable")
			return
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		w.Header().Set("Content-Length", strconv.Itoa(end-start+1))
		w.WriteHeader(206)
		_, _ = w.Write(data[start : end+1])
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(200)
	_, _ = w.Write(data)
}

func parseRange(rng string, size int) (start, end int, ok bool) {
	rng = strings.TrimPrefix(rng, "bytes=")
	parts := strings.SplitN(rng, "-", 2)
	start, _ = strconv.Atoi(parts[0])
	if len(parts) > 1 && parts[1] != "" {
		end, _ = strconv.Atoi(parts[1])
	} else {
		end = size - 1
	}
	if end >= size {
		end = size - 1
	}
	if start > end || start >= size || start < 0 {
		return 0, 0, false
	}
	return start, end, true
}

func (f *fakeGCS) delete(w http.ResponseWriter, r *http.Request, escapedName string) {
	name, err := url.PathUnescape(escapedName)
	if err != nil {
		writeGCSError(w, 400, "bad object name")
		return
	}
	f.mu.Lock()
	_, ok := f.objects[name]
	delete(f.objects, name)
	f.mu.Unlock()
	if !ok {
		writeGCSError(w, 404, "not found")
		return
	}
	w.WriteHeader(204)
}

type listResponse struct {
	Items         []objectResource `json:"items"`
	NextPageToken string           `json:"nextPageToken,omitempty"`
}

// list implements prefix + pageToken pagination the same way
// pkg/store/s3's fake server implements prefix + start-after: pageToken
// here is simply "the last key returned", which is a real GCS server's
// prerogative to make opaque but is a legitimate concrete choice a fake
// only has to satisfy Backend.List's contract (cursor is backend-defined,
// see gcs.go's List doc comment on why the real Backend never parses it).
func (f *fakeGCS) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	prefix := q.Get("prefix")
	pageToken := q.Get("pageToken")
	maxResults := 1000
	if mr := q.Get("maxResults"); mr != "" {
		if n, err := strconv.Atoi(mr); err == nil && n > 0 {
			maxResults = n
		}
	}

	f.mu.Lock()
	var names []string
	for n := range f.objects {
		if strings.HasPrefix(n, prefix) && n > pageToken {
			names = append(names, n)
		}
	}
	f.mu.Unlock()
	sort.Strings(names)

	truncated := false
	if len(names) > maxResults {
		names = names[:maxResults]
		truncated = true
	}

	resp := listResponse{}
	f.mu.Lock()
	for _, n := range names {
		resp.Items = append(resp.Items, f.resource(n, f.objects[n]))
	}
	f.mu.Unlock()
	if truncated && len(names) > 0 {
		resp.NextPageToken = names[len(names)-1]
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	_ = json.NewEncoder(w).Encode(resp)
}
