// Package upload copies files produced by the proxy (currently the
// comparison report) to object storage so they outlive the process and
// its local disk.
package upload

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"golang.org/x/oauth2/google"
)

// defaultGCSEndpoint is the JSON API base used for media uploads.
const defaultGCSEndpoint = "https://storage.googleapis.com"

// gcsScope is the narrowest scope that allows creating objects.
const gcsScope = "https://www.googleapis.com/auth/devstorage.read_write"

// GCSUploader uploads a local file to gs://Bucket/<Prefix>/<Instance>/<name>.
type GCSUploader struct {
	Bucket string
	// Prefix is prepended to every object name (slashes trimmed). Empty
	// means the bucket root.
	Prefix string
	// Instance separates the objects of each process (e.g. the pod name),
	// so replicas never overwrite each other.
	Instance string

	// Client sends the upload request. Nil uses Application Default
	// Credentials (Workload Identity on GKE).
	Client *http.Client
	// Endpoint overrides the API base URL; for tests.
	Endpoint string
	// Now returns the time used in the object name; for tests.
	Now func() time.Time
}

// ObjectName returns the object name UploadGzip writes for base at t:
// <prefix>/<instance>/<base>-<UTC timestamp>.gz.
func (u *GCSUploader) ObjectName(base string, t time.Time) string {
	name := fmt.Sprintf("%s-%s.gz", base, t.UTC().Format("20060102T150405Z"))
	parts := []string{}
	if p := strings.Trim(u.Prefix, "/"); p != "" {
		parts = append(parts, p)
	}
	if i := strings.Trim(u.Instance, "/"); i != "" {
		parts = append(parts, i)
	}
	parts = append(parts, name)
	return path.Join(parts...)
}

// UploadGzip gzips the file at localPath and uploads it, returning the
// object name. A missing or empty file is not uploaded and returns "".
func (u *GCSUploader) UploadGzip(ctx context.Context, localPath string) (string, error) {
	st, err := os.Stat(localPath)
	if os.IsNotExist(err) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if st.Size() == 0 {
		return "", nil
	}

	client := u.Client
	if client == nil {
		client, err = google.DefaultClient(ctx, gcsScope)
		if err != nil {
			return "", fmt.Errorf("gcs credentials: %w", err)
		}
	}
	endpoint := u.Endpoint
	if endpoint == "" {
		endpoint = defaultGCSEndpoint
	}
	now := time.Now
	if u.Now != nil {
		now = u.Now
	}
	object := u.ObjectName(path.Base(localPath), now())

	f, err := os.Open(localPath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	// Stream gzip into the request body so the whole report is never
	// held in memory.
	pr, pw := io.Pipe()
	go func() {
		gz := gzip.NewWriter(pw)
		_, err := io.Copy(gz, f)
		if cerr := gz.Close(); err == nil {
			err = cerr
		}
		pw.CloseWithError(err)
	}()

	q := url.Values{"uploadType": {"media"}, "name": {object}}
	reqURL := fmt.Sprintf("%s/upload/storage/v1/b/%s/o?%s", endpoint, url.PathEscape(u.Bucket), q.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, pr)
	if err != nil {
		pr.Close()
		return "", err
	}
	req.Header.Set("Content-Type", "application/gzip")

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("gcs upload: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return "", fmt.Errorf("gcs upload: %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	return object, nil
}
