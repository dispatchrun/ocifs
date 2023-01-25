package ocifs_test

import (
	"io/fs"
	"testing"

	"github.com/stealthrocket/fstest"
	"github.com/stealthrocket/ocifs"
)

func TestLayerFS(t *testing.T) {
	file := func(data string) *fstest.MapFile {
		return &fstest.MapFile{Mode: 0444, Data: []byte(data)}
	}

	dir := func() *fstest.MapFile {
		return &fstest.MapFile{Mode: 0555 | fs.ModeDir}
	}

	layer1 := fstest.MapFS{
		"a":       dir(),
		"a/x":     dir(),
		"a/x/one": file("1"),
		"a/x/two": file("2"),
	}

	layer2 := fstest.MapFS{
		"a/b":     dir(),
		"a/x/two": file("-2"), // masks a/x/two in layer1
	}

	layer3 := fstest.MapFS{
		"a/b/c":            dir(),
		"a/b/c/d":          dir(),
		"a/b/c/d/e":        dir(),
		"a/x/.wh..wh..opq": file(""), // masks everything in a/x/*
		"a/x/three":        file("3"),
	}

	layer4 := fstest.MapFS{
		"a/b/c/file-0": file("hello"),
		"a/b/c/d/nope": file("?"),
	}

	layer5 := fstest.MapFS{
		"a/b/c/file-1": file("world"), // union with a/b/c in layer4
		"a/b/c/.wh.d":  dir(),         // masks a/b/c/d/* in layer3/layer4
	}

	expect := fstest.MapFS{
		"a":            dir(),         // layer1
		"a/b":          dir(),         // layer2
		"a/b/c":        dir(),         // layer3
		"a/b/c/file-0": file("hello"), // layer4
		"a/b/c/file-1": file("world"), // layer5
		"a/x":          dir(),         // layer1
		"a/x/three":    file("3"),     // layer3
	}

	layers := ocifs.LayerFS(layer1, layer2, layer3, layer4, layer5)
	if err := fstest.EqualFS(expect, layers); err != nil {
		t.Fatal(err)
	}

	names := make([]string, 0, len(expect))
	for name := range expect {
		names = append(names, name)
	}
	if err := fstest.TestFS(layers, names...); err != nil {
		t.Fatal(err)
	}
}
