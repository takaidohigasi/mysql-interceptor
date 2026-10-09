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

// GCSUploader uploads report files to gs://Bucket/<Prefix>/<UTC hour>/<Instance>/<name>
// (and, for the latest snapshot, gs://Bucket/latest/<Prefix>/<Instance>-<name>).
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

// ObjectName returns the gzip object name for the report file base whose
// data started (or, without rotation, was uploaded) at t:
// <prefix>/<YYYYMMDDHH>/<instance>/<base>-<YYYYMMDDTHHMMSSZ>.gz, both in
// UTC. A rotated segment keeps one name for its whole life, so its hourly
// uploads overwrite each other.
func (u *GCSUploader) ObjectName(base string, t time.Time) string {
	t = t.UTC()
	name := fmt.Sprintf("%s-%s.gz", base, t.Format("20060102T150405Z"))
	return joinObject(u.Prefix, t.Format("2006010215"), u.Instance, name)
}

// LatestObjectName returns the uncompressed object that always holds the
// latest upload of the report file base:
// latest/<prefix>/<instance>-<base>.
func (u *GCSUploader) LatestObjectName(base string) string {
	name := base
	if i := strings.Trim(u.Instance, "/"); i != "" {
		name = i + "-" + base
	}
	return joinObject("latest", u.Prefix, name)
}

// joinObject joins the non-empty parts with "/", trimming their slashes.
func joinObject(parts ...string) string {
	var out []string
	for _, p := range parts {
		if p = strings.Trim(p, "/"); p != "" {
			out = append(out, p)
		}
	}
	return path.Join(out...)
}

// UploadGzip gzips the file at localPath and uploads it as
// ObjectName(<file name>, now), returning the object name. A missing or
// empty file is not uploaded and returns "".
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
	now := time.Now
	if u.Now != nil {
		now = u.Now
	}
	object := u.ObjectName(path.Base(localPath), now())
	return object, u.Upload(ctx, localPath, st.Size(), object, true)
}

// Upload uploads the first size bytes of the file at localPath as object,
// gzipped when gz is true and as uncompressed JSON lines otherwise. An
// existing object of that name is overwritten.
func (u *GCSUploader) Upload(ctx context.Context, localPath string, size int64, object string, gz bool) error {
	client := u.Client
	if client == nil {
		var err error
		client, err = google.DefaultClient(ctx, gcsScope)
		if err != nil {
			return fmt.Errorf("gcs credentials: %w", err)
		}
	}
	endpoint := u.Endpoint
	if endpoint == "" {
		endpoint = defaultGCSEndpoint
	}

	f, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer f.Close()
	// Only the first size bytes: the file may be the live report, which
	// keeps growing (possibly mid-record) while this runs.
	src := io.LimitReader(f, size)

	body := src
	contentType := "application/x-ndjson"
	if gz {
		// Stream gzip into the request body so the whole report is never
		// held in memory.
		pr, pw := io.Pipe()
		go func() {
			zw := gzip.NewWriter(pw)
			_, err := io.Copy(zw, src)
			if cerr := zw.Close(); err == nil {
				err = cerr
			}
			pw.CloseWithError(err)
		}()
		defer pr.Close()
		body = pr
		contentType = "application/gzip"
	}

	q := url.Values{"uploadType": {"media"}, "name": {object}}
	reqURL := fmt.Sprintf("%s/upload/storage/v1/b/%s/o?%s", endpoint, url.PathEscape(u.Bucket), q.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", contentType)

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("gcs upload: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return fmt.Errorf("gcs upload: %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	return nil
}
