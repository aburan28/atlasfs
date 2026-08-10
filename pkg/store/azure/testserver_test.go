package azure

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// fakeAzure is a minimal, in-memory Azure Blob Storage REST server: just
// enough of the real wire protocol (PUT Blob with x-ms-blob-type and
// If-None-Match, GET/HEAD with x-ms-range, DELETE, List Blobs) to exercise
// the real azure-sdk-for-go azblob client end to end. It does not verify
// the account's shared-key/SAS auth — that validates Azure's server, not
// our client, and is out of scope for testing our Backend implementation
// (New uses NewClientWithNoCredential for exactly this reason).
type fakeAzure struct {
	mu        sync.Mutex
	objects   map[string][]byte
	container string
}

func newFakeAzure(container string) *httptest.Server {
	f := &fakeAzure{objects: map[string][]byte{}, container: container}
	return httptest.NewServer(http.HandlerFunc(f.handle))
}

type xmlError struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

// writeAzureError sets x-ms-error-code (which the SDK prefers over the XML
// body — see azcore's ResponseError parsing) and, for methods that carry a
// body, the matching XML. HEAD responses never get a body per HTTP
// semantics, so the header is the only signal available there.
func writeAzureError(w http.ResponseWriter, status int, code, msg string, withBody bool) {
	w.Header().Set("x-ms-error-code", code)
	if withBody {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(status)
		_ = xml.NewEncoder(w).Encode(xmlError{Code: code, Message: msg})
		return
	}
	w.WriteHeader(status)
}

func (f *fakeAzure) handle(w http.ResponseWriter, r *http.Request) {
	prefix := "/" + f.container
	if !strings.HasPrefix(r.URL.Path, prefix) {
		http.NotFound(w, r)
		return
	}
	rest := strings.TrimPrefix(r.URL.Path, prefix)
	rest = strings.TrimPrefix(rest, "/")

	if rest == "" && r.URL.Query().Get("comp") == "list" && r.URL.Query().Get("restype") == "container" {
		f.list(w, r)
		return
	}
	if rest == "" {
		writeAzureError(w, 400, "InvalidQueryParameterValue", "expected a blob name or restype=container&comp=list", true)
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
		writeAzureError(w, 405, "UnsupportedHttpVerb", r.Method, true)
	}
}

func (f *fakeAzure) put(w http.ResponseWriter, r *http.Request, key string) {
	if r.Header.Get("x-ms-blob-type") != "BlockBlob" {
		writeAzureError(w, 400, "InvalidHeaderValue", "expected x-ms-blob-type: BlockBlob (this fake server only implements single-shot Put Blob, not staged blocks)", true)
		return
	}

	body := make([]byte, r.ContentLength)
	if r.ContentLength > 0 {
		if _, err := io.ReadFull(r.Body, body); err != nil {
			writeAzureError(w, 500, "InternalError", err.Error(), true)
			return
		}
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if r.Header.Get("If-None-Match") == "*" {
		if _, exists := f.objects[key]; exists {
			// Azure's Put Blob rejects a conditional create against an
			// existing blob with 409 Conflict / BlobAlreadyExists, not
			// S3's 412 PreconditionFailed — verified against the real
			// SDK's error handling (bloberror.HasCode) in azure_test.go.
			writeAzureError(w, 409, "BlobAlreadyExists", "The specified blob already exists.", true)
			return
		}
	}
	f.objects[key] = body
	w.Header().Set("ETag", fmt.Sprintf(`"%x"`, len(body))) // fake but stable per content length; real Azure ETags are opaque to callers too
	w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
	w.WriteHeader(http.StatusCreated) // Put Blob succeeds with 201, not S3's 200
}

func (f *fakeAzure) get(w http.ResponseWriter, r *http.Request, key string) {
	f.mu.Lock()
	data, ok := f.objects[key]
	f.mu.Unlock()
	if !ok {
		writeAzureError(w, 404, "BlobNotFound", "The specified blob does not exist.", true)
		return
	}

	start, end := 0, len(data)-1
	// The SDK sends the byte range in x-ms-range, not the standard HTTP
	// Range header (see BlobClient.downloadCreateRequest in the SDK
	// source) — this is Azure's actual wire shape, not a guess.
	if rng := r.Header.Get("x-ms-range"); rng != "" {
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
			writeAzureError(w, 416, "InvalidRange", "The range specified is invalid for the current size of the resource.", true)
			return
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		w.Header().Set("Content-Length", strconv.Itoa(end-start+1))
		w.Header().Set("ETag", fmt.Sprintf(`"%x"`, len(data)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(data[start : end+1])
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.Header().Set("ETag", fmt.Sprintf(`"%x"`, len(data)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func (f *fakeAzure) head(w http.ResponseWriter, r *http.Request, key string) {
	f.mu.Lock()
	data, ok := f.objects[key]
	f.mu.Unlock()
	if !ok {
		writeAzureError(w, 404, "BlobNotFound", "The specified blob does not exist.", false)
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.Header().Set("ETag", fmt.Sprintf(`"%x"`, len(data)))
	w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
	w.Header().Set("x-ms-blob-type", "BlockBlob")
	w.WriteHeader(http.StatusOK)
}

func (f *fakeAzure) delete(w http.ResponseWriter, r *http.Request, key string) {
	f.mu.Lock()
	_, existed := f.objects[key]
	delete(f.objects, key)
	f.mu.Unlock()
	if !existed {
		writeAzureError(w, 404, "BlobNotFound", "The specified blob does not exist.", true)
		return
	}
	w.WriteHeader(http.StatusAccepted) // Delete Blob succeeds with 202
}

// Wire shapes below mirror Azure's real ListBlobsFlatSegmentResponse (see
// azure-sdk-for-go's internal/generated/zz_models.go) closely enough for
// the SDK's XML decoder to populate the fields our Backend reads. The
// decoder matches by element name regardless of the root's XML name, so
// this doesn't need to declare "EnumerationResults" via an XMLName tag —
// but it's included anyway since that's what a real request/response
// capture shows.
type blobProperties struct {
	LastModified  string `xml:"Last-Modified"`
	Etag          string `xml:"Etag"`
	ContentLength int64  `xml:"Content-Length"`
}

type blobItem struct {
	Name       string         `xml:"Name"`
	Properties blobProperties `xml:"Properties"`
}

type blobsSegment struct {
	Blob []blobItem `xml:"Blob"`
}

type listResult struct {
	XMLName    xml.Name     `xml:"EnumerationResults"`
	Prefix     string       `xml:"Prefix"`
	Marker     string       `xml:"Marker"`
	MaxResults int          `xml:"MaxResults"`
	Blobs      blobsSegment `xml:"Blobs"`
	NextMarker string       `xml:"NextMarker"`
}

func (f *fakeAzure) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	prefix := q.Get("prefix")
	marker := q.Get("marker")
	maxResults := 5000
	if mr := q.Get("maxresults"); mr != "" {
		if n, err := strconv.Atoi(mr); err == nil && n > 0 {
			maxResults = n
		}
	}

	f.mu.Lock()
	var keys []string
	for k := range f.objects {
		if strings.HasPrefix(k, prefix) && k > marker {
			keys = append(keys, k)
		}
	}
	f.mu.Unlock()
	sort.Strings(keys)

	nextMarker := ""
	if len(keys) > maxResults {
		nextMarker = keys[maxResults-1]
		keys = keys[:maxResults]
	}

	res := listResult{Prefix: prefix, Marker: marker, MaxResults: maxResults, NextMarker: nextMarker}
	f.mu.Lock()
	for _, k := range keys {
		data := f.objects[k]
		res.Blobs.Blob = append(res.Blobs.Blob, blobItem{
			Name: k,
			Properties: blobProperties{
				LastModified:  time.Now().UTC().Format(http.TimeFormat),
				Etag:          fmt.Sprintf(`"%x"`, len(data)),
				ContentLength: int64(len(data)),
			},
		})
	}
	f.mu.Unlock()

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(xml.Header))
	_ = xml.NewEncoder(w).Encode(res)
}
