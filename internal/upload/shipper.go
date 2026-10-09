package upload

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/takaidohigasi/mysql-interceptor/internal/compare"
	"github.com/takaidohigasi/mysql-interceptor/internal/segment"
)

// shipperQueue bounds the events waiting for the shipper. One arrives per
// hour, so this only absorbs a slow upload; a dropped snapshot is replaced
// by the next hour's, and a dropped closed segment is picked up by the
// leftover pass.
const shipperQueue = 16

// Shipper handles the report data a rotating compare.Reporter settles
// every hour (compare.SegmentEvent):
//
//   - With an Uploader, each event is uploaded gzipped to
//     Uploader.ObjectName(<file>, <start>) (one name per segment, so the
//     hourly uploads of a growing segment overwrite each other) and
//     uncompressed to Uploader.LatestObjectName(<file>). A closed segment
//     is removed locally once its gzip upload succeeds; one whose upload
//     failed stays and is retried on the next closed segment and at Close.
//   - Without an Uploader, nothing is uploaded and closed segments stay
//     on disk.
//
// Either way at most Keep closed segments are kept locally; older ones
// are removed.
type Shipper struct {
	path     string
	uploader *GCSUploader
	keep     int
	timeout  time.Duration

	events chan compare.SegmentEvent
	done   chan struct{}
	ctx    context.Context
	cancel context.CancelFunc
	once   sync.Once
}

// NewShipper returns a Shipper for the report at path. uploader may be
// nil; timeout bounds each upload. Call Start, then Close at shutdown.
func NewShipper(path string, uploader *GCSUploader, keep int, timeout time.Duration) *Shipper {
	ctx, cancel := context.WithCancel(context.Background())
	return &Shipper{
		path:     path,
		uploader: uploader,
		keep:     keep,
		timeout:  timeout,
		events:   make(chan compare.SegmentEvent, shipperQueue),
		done:     make(chan struct{}),
		ctx:      ctx,
		cancel:   cancel,
	}
}

// Start handles the events in the background, after a first pass over
// segments left by a previous process.
func (s *Shipper) Start() {
	go func() {
		defer close(s.done)
		s.retryLeftovers("")
		s.prune()
		for ev := range s.events {
			s.handle(ev)
		}
	}()
}

// Notify queues ev without blocking (it is called from the reporter's
// writer goroutine).
func (s *Shipper) Notify(ev compare.SegmentEvent) {
	select {
	case s.events <- ev:
	default:
		slog.Warn("report shipper is behind; dropping an event", "file", ev.Path, "closed", ev.Closed)
	}
}

// Close handles the queued events, retries the segments still on disk,
// and returns. Uploads still running when ctx is done are cancelled.
func (s *Shipper) Close(ctx context.Context) {
	s.once.Do(func() {
		stop := context.AfterFunc(ctx, s.cancel)
		defer stop()
		close(s.events)
		<-s.done
		s.retryLeftovers("")
		s.prune()
		s.cancel()
	})
}

func (s *Shipper) handle(ev compare.SegmentEvent) {
	if ev.Closed {
		// The start-up leftover pass may already have shipped it.
		if _, err := os.Stat(ev.Path); os.IsNotExist(err) {
			return
		}
	}
	if s.uploader != nil {
		base := filepath.Base(s.path)
		ok := s.upload(ev.Path, ev.Size, s.uploader.ObjectName(base, ev.Start), true)
		// latest/ is a convenience copy: its failure is logged but does
		// not keep the segment on disk.
		s.upload(ev.Path, ev.Size, s.uploader.LatestObjectName(base), false)
		if ev.Closed {
			if ok {
				s.remove(ev.Path)
			}
			s.retryLeftovers(ev.Path)
		}
	}
	s.prune()
}

// retryLeftovers uploads the closed segments still on disk (other than
// skip) and removes each one that succeeds.
func (s *Shipper) retryLeftovers(skip string) {
	if s.uploader == nil {
		return
	}
	segs, err := segment.List(s.path)
	if err != nil {
		slog.Error("listing report segments", "file", s.path, "err", err)
		return
	}
	base := filepath.Base(s.path)
	for _, seg := range segs {
		if seg == skip || s.ctx.Err() != nil {
			continue
		}
		start, _ := segment.Start(s.path, seg)
		st, err := os.Stat(seg)
		if err != nil {
			continue
		}
		if s.upload(seg, st.Size(), s.uploader.ObjectName(base, start), true) {
			s.remove(seg)
		}
	}
}

// prune keeps the newest keep closed segments on disk.
func (s *Shipper) prune() {
	segs, err := segment.List(s.path)
	if err != nil || len(segs) <= s.keep {
		return
	}
	for _, seg := range segs[:len(segs)-s.keep] {
		if s.uploader != nil {
			slog.Warn("removing a report segment that was never uploaded", "segment", seg, "keep", s.keep)
		}
		s.remove(seg)
	}
}

func (s *Shipper) upload(localPath string, size int64, object string, gz bool) bool {
	ctx, cancel := context.WithTimeout(s.ctx, s.timeout)
	defer cancel()
	if err := s.uploader.Upload(ctx, localPath, size, object, gz); err != nil {
		slog.Error("report upload failed", "bucket", s.uploader.Bucket, "object", object, "file", localPath, "err", err)
		return false
	}
	slog.Info("report uploaded", "bucket", s.uploader.Bucket, "object", object, "bytes", size)
	return true
}

func (s *Shipper) remove(p string) {
	if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
		slog.Error("removing report segment", "segment", p, "err", err)
	}
}
