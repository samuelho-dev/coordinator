package main

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

type entry struct {
	name string
	kind byte
	link string
}

func makeArchive(t *testing.T, entries []entry) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "package.tgz")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	gz := gzip.NewWriter(f)
	tw := tar.NewWriter(gz)
	for _, e := range entries {
		h := &tar.Header{Name: e.name, Typeflag: e.kind, Linkname: e.link, Mode: 0644}
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestArchiveAllowlist(t *testing.T) {
	host := hostTarget()
	supported := false
	for _, target := range targets {
		if target == host {
			supported = true
		}
	}
	if !supported {
		t.Skip("unsupported host")
	}
	var valid []entry
	for _, name := range []string{"package.json", "README.md", "LICENSE", "dist/launch.cjs", "dist/plugins/pi.js", "dist/plugins/omp.js", "dist/bin/" + host + "/coordinator"} {
		valid = append(valid, entry{name: "package/" + name, kind: tar.TypeReg})
	}
	for _, tc := range []struct {
		name            string
		extra           []entry
		release, wantOK bool
	}{
		{name: "host", wantOK: true},
		{name: "release missing targets", release: true},
		{name: "duplicate", extra: []entry{{name: "package/package.json", kind: tar.TypeReg}}},
		{name: "traversal", extra: []entry{{name: "package/../escape", kind: tar.TypeReg}}},
		{name: "symlink", extra: []entry{{name: "package/dist/plugins/extra.js", kind: tar.TypeSymlink, link: "../../package.json"}}},
		{name: "unexpected", extra: []entry{{name: "package/secret", kind: tar.TypeReg}}},
		{name: "wrong type", extra: []entry{{name: "package/dist/", kind: tar.TypeSymlink, link: ".."}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entries := append(append([]entry{}, valid...), tc.extra...)
			err := archive(makeArchive(t, entries), tc.release)
			if (err == nil) != tc.wantOK {
				t.Fatalf("archive acceptance mismatch: %v", err)
			}
		})
	}
	all := append([]entry{}, valid...)
	for _, target := range targets {
		if target != host {
			all = append(all, entry{name: "package/dist/bin/" + target + "/coordinator", kind: tar.TypeReg})
		}
	}
	if err := archive(makeArchive(t, all), true); err != nil {
		t.Fatalf("complete release rejected: %v", err)
	}
}

func TestInstalledQuery(t *testing.T) {
	root := t.TempDir()
	pkg := filepath.Join(root, "node_modules", "@samuelho-dev", "coordinator")
	if err := os.MkdirAll(pkg, 0700); err != nil {
		t.Fatal(err)
	}
	name := "@samuelho-dev/coordinator"
	rootRow := map[string]any{"name": nil, "location": "", "path": root}
	pkgRow := map[string]any{"name": name, "location": "node_modules/@samuelho-dev/coordinator", "path": pkg}
	for _, tc := range []struct {
		name string
		rows []map[string]any
		want bool
	}{
		{"root and package", []map[string]any{rootRow, pkgRow}, true},
		{"extra dependency", []map[string]any{rootRow, pkgRow, {"name": "other", "location": "node_modules/other", "path": filepath.Join(root, "node_modules", "other")}}, false},
		{"duplicate root", []map[string]any{rootRow, rootRow}, false},
		{"wrong package location", []map[string]any{rootRow, {"name": name, "location": "", "path": pkg}}, false},
		{"missing root location", []map[string]any{{"path": root}, pkgRow}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, err := json.Marshal(tc.rows)
			if err != nil {
				t.Fatal(err)
			}
			if validInstalled(b, root, pkg, name) != tc.want {
				t.Fatal("unexpected query acceptance")
			}
		})
	}
}
