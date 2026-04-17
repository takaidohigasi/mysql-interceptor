package config

import (
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

type OnChangeFunc func(cfg *Config)

type Watcher struct {
	path      string
	fsWatcher *fsnotify.Watcher
	onChange  []OnChangeFunc
	mu        sync.Mutex
	done      chan struct{}
}

func NewWatcher(configPath string) (*Watcher, error) {
	fsw, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	dir := filepath.Dir(configPath)
	if err := fsw.Add(dir); err != nil {
		fsw.Close()
		return nil, err
	}

	w := &Watcher{
		path:      configPath,
		fsWatcher: fsw,
		done:      make(chan struct{}),
	}

	go w.watchLoop()

	return w, nil
}

func (w *Watcher) OnChange(fn OnChangeFunc) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.onChange = append(w.onChange, fn)
}

func (w *Watcher) Close() {
	w.fsWatcher.Close()
	<-w.done
}

func (w *Watcher) watchLoop() {
	defer close(w.done)

	absPath, _ := filepath.Abs(w.path)

	for {
		select {
		case event, ok := <-w.fsWatcher.Events:
			if !ok {
				return
			}
			eventAbs, _ := filepath.Abs(event.Name)
			if eventAbs != absPath {
				continue
			}
			// Many editors save via write-to-temp + rename; cover both
			// the Write-in-place and the atomic-rename cases, plus the
			// Remove event some editors produce before the rename lands.
			reload := event.Op&fsnotify.Write != 0 ||
				event.Op&fsnotify.Create != 0 ||
				event.Op&fsnotify.Rename != 0 ||
				event.Op&fsnotify.Remove != 0
			if !reload {
				continue
			}
			// For rename/remove we need a brief settle window so we read
			// the new file, not a transient empty one.
			if event.Op&(fsnotify.Rename|fsnotify.Remove) != 0 {
				time.Sleep(50 * time.Millisecond)
			}

			slog.Info("config file changed, reloading", "path", w.path)
			cfg, err := Load(w.path)
			if err != nil {
				slog.Error("failed to reload config", "err", err)
				continue
			}

			w.mu.Lock()
			callbacks := make([]OnChangeFunc, len(w.onChange))
			copy(callbacks, w.onChange)
			w.mu.Unlock()

			for _, fn := range callbacks {
				fn(cfg)
			}
			slog.Info("config reloaded successfully")

		case err, ok := <-w.fsWatcher.Errors:
			if !ok {
				return
			}
			slog.Error("config watcher error", "err", err)
		}
	}
}
