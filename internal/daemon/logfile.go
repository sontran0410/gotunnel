package daemon

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// RotatingFile is an append-only log file that is rotated when it grows past
// maxSize: path → path.1 → … → path.<backups>.
type RotatingFile struct {
	mu      sync.Mutex
	path    string
	maxSize int64
	backups int
	f       *os.File
	size    int64
}

// OpenRotating opens (or creates) a rotating log file.
func OpenRotating(path string, maxSize int64, backups int) (*RotatingFile, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	r := &RotatingFile{path: path, maxSize: maxSize, backups: backups}
	if err := r.open(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *RotatingFile) open() error {
	f, err := os.OpenFile(r.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	r.f, r.size = f, fi.Size()
	return nil
}

// Write appends p, rotating first if the file would grow too large.
func (r *RotatingFile) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.f == nil {
		return 0, os.ErrClosed
	}
	if r.size > 0 && r.size+int64(len(p)) > r.maxSize {
		if err := r.rotate(); err != nil {
			return 0, err
		}
	}
	n, err := r.f.Write(p)
	r.size += int64(n)
	return n, err
}

func (r *RotatingFile) rotate() error {
	r.f.Close()
	for i := r.backups - 1; i >= 1; i-- {
		_ = os.Rename(fmt.Sprintf("%s.%d", r.path, i), fmt.Sprintf("%s.%d", r.path, i+1))
	}
	if r.backups > 0 {
		_ = os.Rename(r.path, r.path+".1")
	} else {
		_ = os.Remove(r.path)
	}
	return r.open()
}

// Close closes the file.
func (r *RotatingFile) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.f == nil {
		return nil
	}
	err := r.f.Close()
	r.f = nil
	return err
}

// Follow writes the last n lines of the log at path to w and, if follow is
// set, keeps copying new lines until ctx ends, reopening the file when it is
// rotated.
func Follow(ctx context.Context, path string, n int, follow bool, w io.Writer) error {
	if t := TailLines(path, n); t != "" {
		fmt.Fprintln(w, t)
	} else if _, err := os.Stat(path); err != nil && !follow {
		return err
	}
	if !follow {
		return nil
	}
	var f *os.File
	var off int64
	reopen := func(fromEnd bool) {
		if f != nil {
			f.Close()
			f = nil
		}
		nf, err := os.Open(path)
		if err != nil {
			return
		}
		f, off = nf, 0
		if fromEnd {
			off, _ = f.Seek(0, io.SeekEnd)
		}
	}
	reopen(true)
	defer func() {
		if f != nil {
			f.Close()
		}
	}()
	buf := make([]byte, 32<<10)
	for {
		if f != nil {
			for {
				nr, err := f.Read(buf)
				if nr > 0 {
					w.Write(buf[:nr])
					off += int64(nr)
				}
				if err != nil || nr == 0 {
					break
				}
			}
			// Rotated or truncated: start over on the new file.
			if fi, err := os.Stat(path); err == nil {
				if cur, err := f.Stat(); err == nil && (!os.SameFile(fi, cur) || fi.Size() < off) {
					reopen(false)
					continue
				}
			}
		} else {
			reopen(false)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(500 * time.Millisecond):
		}
	}
}
