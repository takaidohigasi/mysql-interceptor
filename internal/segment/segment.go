// Package segment names and finds the closed segments of a rotated
// report file: <path>-<UTC start timestamp>, e.g.
// diff-report.jsonl-20261009T090000Z. The timestamp is when the segment
// started, so names sort chronologically.
package segment

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// timeLayout is the UTC timestamp suffix of a segment name.
const timeLayout = "20060102T150405Z"

// Name returns the segment name for the report at path started at start.
func Name(path string, start time.Time) string {
	return path + "-" + start.UTC().Format(timeLayout)
}

// Start parses the start time out of segment, a segment of the report at
// path. ok is false when segment is not one.
func Start(path, segment string) (time.Time, bool) {
	rest, found := strings.CutPrefix(segment, path+"-")
	if !found {
		return time.Time{}, false
	}
	t, err := time.Parse(timeLayout, rest)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// List returns the segments of the report at path, oldest first. A
// missing directory is not an error.
func List(path string) ([]string, error) {
	entries, err := os.ReadDir(filepath.Dir(path))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		p := filepath.Join(filepath.Dir(path), e.Name())
		if _, ok := Start(path, p); ok {
			out = append(out, p)
		}
	}
	sort.Strings(out)
	return out, nil
}
