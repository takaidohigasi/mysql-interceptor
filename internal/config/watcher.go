package config

import (
	"log"
	"path/filepath"
	"sync"

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
			if event.Op&(fsnotify.Write|fsnotify.Create) == 0 {
				continue
			}

			log.Printf("config file changed, reloading...")
			cfg, err := Load(w.path)
			if err != nil {
				log.Printf("failed to reload config: %v", err)
				continue
			}

			w.mu.Lock()
			callbacks := make([]OnChangeFunc, len(w.onChange))
			copy(callbacks, w.onChange)
			w.mu.Unlock()

			for _, fn := range callbacks {
				fn(cfg)
			}
			log.Printf("config reloaded successfully")

		case err, ok := <-w.fsWatcher.Errors:
			if !ok {
				return
			}
			log.Printf("config watcher error: %v", err)
		}
	}
}
