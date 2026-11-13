package coordinator

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

const maxPrivateBytes = 65536

func canonical(path string) bool {
	return filepath.IsAbs(path) && filepath.Clean(path) == path && !strings.ContainsAny(path, "\x00\r\n")
}
func owner(info os.FileInfo) (uint32, bool) {
	s, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return s.Uid, true
}
func checkDir(path string, exact bool) error {
	if !canonical(path) {
		return errors.New("unsafe private path")
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	uid, ok := owner(info)
	real, err := filepath.EvalSymlinks(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || !ok || uid != uint32(os.Getuid()) || real != path || (exact && info.Mode().Perm() != 0700) || (!exact && info.Mode().Perm()&0077 != 0) {
		return errors.New("unsafe private directory")
	}
	return nil
}
func readPrivate(file string, exact bool) ([]byte, error) {
	if !canonical(file) {
		return nil, errors.New("unsafe private path")
	}
	if err := checkDir(filepath.Dir(file), false); err != nil {
		return nil, err
	}
	fd, err := syscall.Open(file, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), file)
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	uid, ok := owner(info)
	if !info.Mode().IsRegular() || !ok || uid != uint32(os.Getuid()) || (exact && info.Mode().Perm() != 0600) || (!exact && info.Mode().Perm()&0077 != 0) || info.Size() > maxPrivateBytes {
		return nil, errors.New("unsafe private file")
	}
	b, err := io.ReadAll(io.LimitReader(f, maxPrivateBytes+1))
	if err != nil {
		return nil, err
	}
	if len(b) > maxPrivateBytes {
		return nil, errors.New("private file exceeds bound")
	}
	return b, nil
}
func ReadPrivate(file string) ([]byte, error) { return readPrivate(file, false) }

// WritePrivate never replaces an existing file and rolls back only its own creations.
func WritePrivate(directory string, files map[string][]byte) (err error) {
	if !canonical(directory) {
		return errors.New("unsafe directory")
	}
	var dirs, created []string
	defer func() {
		if err != nil {
			for i := len(created) - 1; i >= 0; i-- {
				_ = os.Remove(created[i])
			}
			for i := len(dirs) - 1; i >= 0; i-- {
				_ = os.Remove(dirs[i])
			}
		}
	}()
	var ensure func(string, bool) error
	ensure = func(path string, private bool) error {
		info, e := os.Lstat(path)
		if errors.Is(e, os.ErrNotExist) {
			if path == filepath.Dir(path) {
				return e
			}
			if e = ensure(filepath.Dir(path), false); e != nil {
				return e
			}
			if e = os.Mkdir(path, 0700); e == nil {
				dirs = append(dirs, path)
			} else if !errors.Is(e, os.ErrExist) {
				return e
			}
			info, e = os.Lstat(path)
		}
		if e != nil {
			return e
		}
		real, e := filepath.EvalSymlinks(path)
		if e != nil {
			return e
		}
		uid, ok := owner(info)
		if !info.IsDir() || real != path || !ok {
			return errors.New("unsafe directory")
		}
		if private {
			if uid != uint32(os.Getuid()) || info.Mode().Perm() != 0700 {
				return errors.New("unsafe private directory")
			}
		} else if (uid != uint32(os.Getuid()) && uid != 0) || (info.Mode().Perm()&0022 != 0 && info.Mode()&os.ModeSticky == 0) {
			return errors.New("unsafe ancestor")
		}
		return nil
	}
	if err = ensure(directory, true); err != nil {
		return err
	}
	for p := filepath.Dir(directory); ; p = filepath.Dir(p) {
		if err = ensure(p, false); err != nil {
			return err
		}
		if p == filepath.Dir(p) {
			break
		}
	}
	for name, data := range files {
		file := name
		if !filepath.IsAbs(file) {
			file = filepath.Join(directory, name)
		}
		if !canonical(file) || filepath.Dir(file) != directory || len(data) > maxPrivateBytes {
			return errors.New("unsafe destination")
		}
		fd, e := syscall.Open(file, syscall.O_WRONLY|syscall.O_CREAT|syscall.O_EXCL|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0600)
		if e != nil {
			return e
		}
		created = append(created, file)
		f := os.NewFile(uintptr(fd), file)
		if e = f.Chmod(0600); e == nil {
			_, e = f.Write(data)
		}
		closeErr := f.Close()
		if e != nil {
			return fmt.Errorf("writing private file: %w", e)
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return nil
}
