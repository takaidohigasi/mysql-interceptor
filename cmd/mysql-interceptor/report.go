package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/takaidohigasi/mysql-interceptor/internal/compare"
)

// runReport implements `mysql-interceptor report`: it reads a comparison
// report (comparison.output_file, plain or .gz) and prints the differing
// records or a per-digest summary. It needs no shell or extra tools in the
// container, so it works on distroless images via `kubectl exec`.
func runReport() {
	if err := report(os.Args[2:], os.Stdout); err != nil {
		fatal("report error", "err", err)
	}
}

func report(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("report", flag.ContinueOnError)
	file := fs.String("file", "", "comparison report to read (comparison.output_file; .gz is decompressed)")
	summary := fs.Bool("summary", false, "print a per-digest summary instead of individual records")
	showValues := fs.Bool("show-values", false, "include query text and original/replay values (may contain sensitive data)")
	digest := fs.String("digest", "", "only records whose digest contains this substring (case-insensitive)")
	since := fs.Duration("since", 0, "only records newer than this duration, e.g. 1h (0 = all)")
	limit := fs.Int("limit", 0, "print at most this many records, newest last (0 = all; ignored with --summary)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *file == "" {
		return fmt.Errorf("--file is required")
	}

	opts := compare.ReportViewOptions{Digest: *digest, ShowValues: *showValues}
	if *since > 0 {
		opts.Since = time.Now().Add(-*since)
	}

	rc, err := compare.OpenReport(*file)
	if err != nil {
		return err
	}
	defer rc.Close()

	records, skipped, err := compare.ReadReport(rc, opts)
	if err != nil {
		return err
	}

	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	if *summary {
		fmt.Fprintln(w, "DIGEST\tRECORDS\tDIFFERED\tTYPES\tCOLUMNS\tFIRST\tLAST")
		for _, s := range compare.SummarizeReport(records) {
			fmt.Fprintf(w, "%s\t%d\t%d\t%s\t%s\t%s\t%s\n",
				s.Digest, s.Records, s.Differed,
				compare.FormatCounts(s.Types), compare.FormatCounts(s.Columns),
				s.FirstSeen.UTC().Format(time.RFC3339), s.LastSeen.UTC().Format(time.RFC3339))
		}
	} else {
		if *limit > 0 && len(records) > *limit {
			records = records[len(records)-*limit:]
		}
		fmt.Fprintln(w, "TIME\tDIGEST\tUSER\tORIG_MS\tREPLAY_MS\tDIFFERENCES")
		for _, rec := range records {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n",
				rec.Timestamp.UTC().Format(time.RFC3339), rec.QueryDigest, rec.User,
				strconv.FormatFloat(rec.OriginalTimeMs, 'f', 2, 64),
				strconv.FormatFloat(rec.ReplayTimeMs, 'f', 2, 64),
				formatDifferences(rec.Differences))
			if *showValues {
				fmt.Fprintf(w, "\tquery: %s\t\t\t\t\n", rec.Query)
				for _, d := range rec.Differences {
					fmt.Fprintf(w, "\t  %s row=%d col=%s original=%q replay=%q\t\t\t\t\n",
						d.Type, d.Row, d.Column, d.Original, d.Replay)
				}
			}
		}
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if skipped > 0 {
		fmt.Fprintf(out, "(skipped %d malformed line(s))\n", skipped)
	}
	return nil
}

// formatDifferences renders the differences of one record without their
// values: "type[column]" per difference, or "-" when there are none.
func formatDifferences(diffs []compare.Difference) string {
	if len(diffs) == 0 {
		return "-"
	}
	s := ""
	for i, d := range diffs {
		if i > 0 {
			s += ", "
		}
		s += d.Type
		if d.Column != "" {
			s += "[" + d.Column + "]"
		}
	}
	return s
}
