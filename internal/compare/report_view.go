package compare

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"
)

// hiddenValue replaces cell values and query text in report views unless
// the caller asks for values. Diff records can carry row data (PII,
// credentials), so the default view shows only what drifted, not the data.
const hiddenValue = "<hidden>"

// ReportViewOptions filters and shapes the records read by ReadReport.
type ReportViewOptions struct {
	// Since drops records older than this instant. Zero keeps everything.
	Since time.Time
	// Digest keeps only records whose query_digest contains this
	// substring (case-insensitive). Empty keeps everything.
	Digest string
	// ShowValues keeps the query text and the original/replay values of
	// each difference. When false they are replaced with "<hidden>".
	ShowValues bool
}

// OpenReport opens a comparison report written by Reporter. Files ending
// in ".gz" are decompressed transparently. The caller must Close the
// returned reader.
func OpenReport(path string) (io.ReadCloser, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	if !strings.HasSuffix(path, ".gz") {
		return f, nil
	}
	gz, err := gzip.NewReader(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("opening gzip report: %w", err)
	}
	return &gzipFile{Reader: gz, f: f}, nil
}

type gzipFile struct {
	*gzip.Reader
	f *os.File
}

func (g *gzipFile) Close() error {
	err := g.Reader.Close()
	if cerr := g.f.Close(); err == nil {
		err = cerr
	}
	return err
}

// ReadReport parses the JSONL written by Reporter and returns the
// comparison records that pass opts, in file order. Heartbeat lines are
// skipped. Values are hidden unless opts.ShowValues is set. Malformed
// lines are counted in the returned skipped count rather than failing
// the whole read, since a report cut off mid-line (process killed while
// writing) is expected.
func ReadReport(r io.Reader, opts ReportViewOptions) (records []CompareResult, skipped int, err error) {
	digest := strings.ToLower(opts.Digest)
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64<<10), 16<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var probe struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(line, &probe); err != nil {
			skipped++
			continue
		}
		if probe.Type != "" {
			// Heartbeat (or any future typed line): not a comparison.
			continue
		}
		var rec CompareResult
		if err := json.Unmarshal(line, &rec); err != nil {
			skipped++
			continue
		}
		if !opts.Since.IsZero() && rec.Timestamp.Before(opts.Since) {
			continue
		}
		if digest != "" && !strings.Contains(strings.ToLower(rec.QueryDigest), digest) {
			continue
		}
		if !opts.ShowValues {
			rec.Query = hiddenValue
			for i := range rec.Differences {
				rec.Differences[i].Original = hiddenValue
				rec.Differences[i].Replay = hiddenValue
			}
		}
		records = append(records, rec)
	}
	if err := sc.Err(); err != nil {
		return records, skipped, fmt.Errorf("reading report: %w", err)
	}
	return records, skipped, nil
}

// DigestDiffSummary aggregates the differing records of one digest.
type DigestDiffSummary struct {
	Digest string
	// Records is the number of comparison records for the digest;
	// Differed counts the ones that did not match.
	Records  int
	Differed int
	// Types counts differences by type (row_count, cell_value, error, ...).
	Types map[string]int
	// Columns counts cell differences by column name.
	Columns   map[string]int
	FirstSeen time.Time
	LastSeen  time.Time
}

// SummarizeReport groups records by digest, ordered by differed count
// (descending), then digest.
func SummarizeReport(records []CompareResult) []DigestDiffSummary {
	byDigest := make(map[string]*DigestDiffSummary)
	for _, rec := range records {
		s, ok := byDigest[rec.QueryDigest]
		if !ok {
			s = &DigestDiffSummary{
				Digest:    rec.QueryDigest,
				Types:     make(map[string]int),
				Columns:   make(map[string]int),
				FirstSeen: rec.Timestamp,
				LastSeen:  rec.Timestamp,
			}
			byDigest[rec.QueryDigest] = s
		}
		s.Records++
		if !rec.Match && !rec.Ignored {
			s.Differed++
		}
		for _, d := range rec.Differences {
			s.Types[d.Type]++
			if d.Column != "" {
				s.Columns[d.Column]++
			}
		}
		if rec.Timestamp.Before(s.FirstSeen) {
			s.FirstSeen = rec.Timestamp
		}
		if rec.Timestamp.After(s.LastSeen) {
			s.LastSeen = rec.Timestamp
		}
	}
	out := make([]DigestDiffSummary, 0, len(byDigest))
	for _, s := range byDigest {
		out = append(out, *s)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Differed != out[j].Differed {
			return out[i].Differed > out[j].Differed
		}
		return out[i].Digest < out[j].Digest
	})
	return out
}

// FormatCounts renders a count map as "k1=n1, k2=n2" sorted by key, or
// "-" when empty.
func FormatCounts(m map[string]int) string {
	if len(m) == 0 {
		return "-"
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", k, m[k]))
	}
	return strings.Join(parts, ", ")
}
