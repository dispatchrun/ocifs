package ocifs

import (
	"errors"
	"io"
	"io/fs"
	"path"
	"strings"

	"github.com/stealthrocket/fslink"
)

const (
	whiteoutPrefix = ".wh."
	whiteoutOpaque = ".wh..wh..opq"
)

// LayerFS constructs a read-only overlay file system by stacking layers of OCI
// images.
//
// Because OCI images are immutable objects, the resulting layered file system
// is read-only. Write permissions are removed on all files exposed by the file
// system and the fs.File instances have no methods to allow writes.
//
// The underlying fs.FS layers are expected to support symlinks by exposing a
// ReadLink method with this signature:
//
//	type ReadLinkFS interface {
//		ReadLink(name string) (string, error)
//	}
//
// Files opened by a layered file system implement fs.ReadFileFS, io.ReaderAt,
// and io.Seeker. If the underlying files do not support these extensions of the
// fs.File interface, and fs.PathError wrapping fs.ErrInvalid is returned.
func LayerFS(layers ...fs.FS) fs.FS {
	layers = append([]fs.FS{}, layers...)
	// Reverse the layers so we can use range loops to iterate the list in the
	// right priority order.
	for i, j := 0, len(layers)-1; i < j; {
		layers[i], layers[j] = layers[j], layers[i]
		i++
		j--
	}
	return layerFS(layers)
}

type layerFS []fs.FS

func (layers layerFS) Open(name string) (fs.File, error) {
	visibleLayers, err := layers.lookup("open", name)
	if err != nil {
		return nil, err
	}

	files := make([]fs.File, 0, len(layers))
	defer func() {
		for _, f := range files {
			f.Close()
		}
	}()

	for _, layer := range visibleLayers {
		f, err := layer.Open(name)
		if err != nil {
			return nil, err
		}
		files = append(files, f)
	}

	defer func() { files = nil }()
	return &layerFile{layers: files, name: name}, nil
}

func (layers layerFS) Sub(name string) (fs.FS, error) {
	visibleLayers, err := layers.lookup("open", name)
	if err != nil {
		return nil, err
	}
	for i, layer := range visibleLayers {
		layer, err := fslink.Sub(layer, name)
		if err != nil {
			return nil, err
		}
		visibleLayers[i] = layer
	}
	return layerFS(visibleLayers), nil
}

func (layers layerFS) ReadLink(name string) (string, error) {
	visibleLayers, err := layers.lookup("readlink", name)
	if err != nil {
		return "", err
	}

	for _, layer := range visibleLayers {
		link, err := fslink.ReadLink(layer, name)
		switch {
		case err == nil:
			return link, nil
		case errors.Is(err, fs.ErrNotExist):
		case errors.Is(err, fs.ErrInvalid):
		default:
			return "", err
		}
	}

	return "", &fs.PathError{"readlink", name, fs.ErrNotExist}
}

func (layers layerFS) lookup(op, name string) ([]fs.FS, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{op, name, fs.ErrNotExist}
	}
	if name == "." {
		return layers, nil
	}
	visibleLayers := append([]fs.FS{}, layers...)
	// To determine if a layer is masking the ones below, we have to walk
	// through each element of the path and determine if any of the upper
	// layer has whiteout files that would mask the lower layers.
	path := name
	walk := 0

	for walk < len(path) && len(visibleLayers) > 0 {
		if i := strings.IndexByte(path[walk:], '/'); i < 0 {
			walk = len(path)
		} else {
			walk = walk + i
		}

		whiteoutOne, whiteoutAll := whiteout(path[:walk])

		for i := 0; i < len(visibleLayers); {
			s, err := fs.Stat(visibleLayers[i], path[:walk])
			if err != nil {
				if !errors.Is(err, fs.ErrNotExist) {
					return nil, err
				}
				// The layer does not have the file, it cannot be part of the
				// visible layers.
				n := copy(visibleLayers[i:], visibleLayers[i+1:])
				visibleLayers = visibleLayers[:i+n]
				continue
			} else if !s.IsDir() {
				// The layer is not a directory, it will mask all the files in
				// layers below. However, if this is not the top most layer it
				// indicates that the previous layers contained directories and
				// therefore the current layer cannot be included.
				if i == 0 {
					i++
				}
				visibleLayers = visibleLayers[:i]
				break
			}

			if exist, err := hasOneOf(visibleLayers[i], whiteoutOne, whiteoutAll); err != nil {
				return nil, err
			} else if exist {
				// The layer has whiteout files that mask all the layers below,
				// we strip them out of the list of visible layers.
				visibleLayers = visibleLayers[:i+1]
				break
			}
			i++
		}
		walk++
	}

	if len(visibleLayers) == 0 {
		return nil, &fs.PathError{op, name, fs.ErrNotExist}
	}
	return visibleLayers, nil
}

var (
	_ fs.SubFS          = (layerFS)(nil)
	_ fslink.ReadLinkFS = (layerFS)(nil)
)

func whiteout(name string) (whiteoutOne, whiteoutAll string) {
	dir, base := path.Split(name)
	whiteoutOne = path.Join(dir, whiteoutPrefix+base)
	whiteoutAll = path.Join(dir, whiteoutOpaque)
	return
}

func hasOneOf(fsys fs.FS, names ...string) (bool, error) {
	for _, name := range names {
		_, err := fs.Stat(fsys, name)
		if err == nil {
			return true, nil
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return false, err
		}
	}
	return false, nil
}

type layerFile struct {
	layers []fs.File
	name   string
	// lazily allocated by ReadDir
	dirReader *dirReader
}

func (f *layerFile) Close() error {
	for _, layer := range f.layers {
		layer.Close()
	}
	return nil
}

func (f *layerFile) Stat() (fs.FileInfo, error) {
	s, err := f.layers[0].Stat()
	if err != nil {
		return nil, err
	}
	return &layerInfo{s}, nil
}

func (f *layerFile) Read(b []byte) (int, error) {
	return f.layers[0].Read(b)
}

func (f *layerFile) ReadAt(b []byte, offset int64) (int, error) {
	if r, ok := f.layers[0].(io.ReaderAt); ok {
		return r.ReadAt(b, offset)
	}
	return 0, &fs.PathError{"read", f.name, fs.ErrInvalid}
}

func (f *layerFile) Seek(offset int64, whence int) (int64, error) {
	if s, ok := f.layers[0].(io.Seeker); ok {
		offset, err := s.Seek(offset, whence)
		if err != nil {
			return offset, err
		}
		if offset == 0 && f.dirReader != nil {
			// Using lseek to reset the directory position is supported by posix
			// so we be good citizens and comply, assuming that if we get to
			// this stage when Seek was called on a directory we must be running
			// on a posix complient environment.
			f.dirReader = nil
		}
		return offset, nil
	}
	return 0, &fs.PathError{"seek", f.name, fs.ErrInvalid}
}

func (f *layerFile) ReadDir(n int) ([]fs.DirEntry, error) {
	if f.dirReader == nil {
		files := make([]fs.ReadDirFile, 0, len(f.layers))
		for _, layer := range f.layers {
			if f, ok := layer.(fs.ReadDirFile); ok {
				files = append(files, f)
			}
		}
		f.dirReader = &dirReader{files: files}
	}
	if n < 0 {
		n = 0
	}
	ret := make([]fs.DirEntry, 0, n)
	err := f.dirReader.scan(n, func(e fs.DirEntry) error {
		ret = append(ret, e)
		return nil
	})
	return ret, err
}

var (
	_ fs.ReadDirFile = (*layerFile)(nil)
	_ io.ReaderAt    = (*layerFile)(nil)
	_ io.Seeker      = (*layerFile)(nil)
)

type layerInfo struct{ fs.FileInfo }

func (info *layerInfo) Mode() fs.FileMode {
	// Layers are read-only, so mask all write permissions on the files to let
	// the application know that it is not allowed to write those layers.
	mode := info.FileInfo.Mode()
	mode &= ^fs.FileMode(0222)
	return mode
}

type dirReader struct {
	files []fs.ReadDirFile
	names []string
	masks map[string]struct{}
}

func (dir *dirReader) scan(n int, f func(fs.DirEntry) error) error {
	if dir.masks == nil {
		dir.masks = make(map[string]struct{})
	}

	dirents := 0
	for len(dir.files) > 0 {
		for {
			entries, err := dir.files[0].ReadDir(n - dirents)

			for _, entry := range entries {
				name := entry.Name()
				if _, seen := dir.masks[name]; seen {
					continue
				}
				switch {
				case name == whiteoutOpaque:
					dir.files = dir.files[:1]
				case strings.HasPrefix(name, whiteoutPrefix):
					dir.names = append(dir.names, name[len(whiteoutPrefix):])
				default:
					dir.names = append(dir.names, name)
					if err := f(entry); err != nil {
						return err
					}
					dirents++
				}
			}

			if n == 0 || err == io.EOF {
				break
			}
			if n == dirents || err != nil {
				return err
			}
		}

		// Apply names after completing iteration of the layer otherwise
		// it could end up mistakenly masking its own entries.
		for _, name := range dir.names {
			dir.masks[name] = struct{}{}
		}
		dir.names = dir.names[:0]
		dir.files = dir.files[1:]
	}

	if dirents < n {
		return io.EOF
	}
	return nil
}
