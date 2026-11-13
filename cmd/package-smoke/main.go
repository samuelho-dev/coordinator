package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

var targets = []string{"linux-x64", "linux-arm64", "darwin-x64", "darwin-arm64"}

func hostTarget() string {
	arch := runtime.GOARCH
	if arch == "amd64" {
		arch = "x64"
	}
	return runtime.GOOS + "-" + arch
}

func archive(file string, release bool) error {
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer gz.Close()
	allowed := map[string]bool{}
	for _, name := range []string{"package.json", "README.md", "LICENSE", "dist/launch.cjs", "dist/plugins/pi.js", "dist/plugins/omp.js"} {
		allowed["package/"+name] = true
	}
	for _, target := range targets {
		allowed["package/dist/bin/"+target+"/coordinator"] = true
	}
	dirs := map[string]bool{"package/": true, "package/dist/": true, "package/dist/plugins/": true, "package/dist/bin/": true}
	for _, target := range targets {
		dirs["package/dist/bin/"+target+"/"] = true
	}
	seen := map[string]bool{}
	limited := &io.LimitedReader{R: gz, N: (512 << 20) + 1}
	tr := tar.NewReader(limited)
	for {
		h, e := tr.Next()
		if e == io.EOF {
			break
		}
		if e != nil {
			return e
		}
		if seen[h.Name] {
			return errors.New("duplicate archive entry")
		}
		seen[h.Name] = true
		if h.Typeflag == tar.TypeDir && dirs[h.Name] {
			continue
		}
		if (h.Typeflag != tar.TypeReg && h.Typeflag != tar.TypeRegA) || !allowed[h.Name] || h.Size < 0 || h.Size > 128<<20 {
			return errors.New("unexpected archive entry")
		}
		if _, e = io.Copy(io.Discard, tr); e != nil {
			return e
		}
	}
	padding, e := io.ReadAll(io.LimitReader(limited, 1<<20))
	if e != nil || limited.N <= 0 || len(padding) == 1<<20 || len(bytes.Trim(padding, "\x00")) != 0 {
		return errors.New("invalid archive trailer")
	}
	for name := range allowed {
		if strings.Contains(name, "/bin/") {
			continue
		}
		if !seen[name] {
			return errors.New("required archive file missing")
		}
	}
	required := []string{hostTarget()}
	if release {
		required = targets
	}
	for _, target := range required {
		if !seen["package/dist/bin/"+target+"/coordinator"] {
			return errors.New("required platform binary missing")
		}
	}
	return nil
}

func run(dir string, env []string, timeout time.Duration, name string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	cmd.Env = env
	var out, stderr bytes.Buffer
	cmd.Stdout = &limitedWriter{w: &out, remaining: 1 << 20}
	cmd.Stderr = &limitedWriter{w: &stderr, remaining: 1 << 20}
	if err := cmd.Run(); err != nil {
		if name == "go" {
			for _, line := range bytes.Split(stderr.Bytes(), []byte("\n")) {
				var result struct {
					Scenario string `json:"scenario"`
					Status   string `json:"status"`
					Message  string `json:"message"`
				}
				if json.Unmarshal(line, &result) == nil && result.Scenario == "go-smoke" && result.Status == "failed" && safeSmokeMessages[result.Message] {
					return nil, errors.New(result.Message)
				}
			}
		}
		return nil, errors.New("subprocess failed")
	}
	return out.Bytes(), nil
}

type limitedWriter struct {
	w         io.Writer
	remaining int
}

func (w *limitedWriter) Write(p []byte) (int, error) {
	if len(p) > w.remaining {
		return 0, errors.New("output exceeds limit")
	}
	n, e := w.w.Write(p)
	w.remaining -= n
	return n, e
}

var safeSmokeMessages = map[string]bool{
	"inspector readiness deadline exceeded": true, "native announce deadline exceeded": true,
	"pi host executable unavailable": true, "omp host executable unavailable": true,
	"native install failed": true, "invalid native install": true, "package plugin unavailable": true,
	"native host start failed": true, "native startup deadline exceeded": true,
	"native host announce deadline exceeded": true, "native list acknowledgment deadline exceeded": true,
	"native list message deadline exceeded": true, "unsafe native host output": true,
	"native list started model turn": true, "native host exit failed": true,
	"MCP connection failed": true, "session.list failed": true, "session.inspect failed": true,
	"native inspection mismatch": true, "unauthenticated request accepted": true,
}

type stageError struct{ stage, detail string }

func (e stageError) Error() string { return e.stage }
func stageFailure(stage string, err error) error {
	detail := ""
	if stage == "smoke" && err != nil && safeSmokeMessages[err.Error()] {
		detail = err.Error()
	}
	return stageError{stage: stage, detail: detail}
}

func validInstalled(query []byte, root, pkg, name string) bool {
	var rows []struct {
		Name     *string `json:"name"`
		Path     string  `json:"path"`
		Location *string `json:"location"`
	}
	if json.Unmarshal(query, &rows) != nil || len(rows) != 2 {
		return false
	}
	root, err := filepath.EvalSymlinks(root)
	if err != nil {
		return false
	}
	pkg, err = filepath.EvalSymlinks(pkg)
	if err != nil {
		return false
	}
	foundRoot, foundPackage := false, false
	for _, row := range rows {
		if !filepath.IsAbs(row.Path) || row.Location == nil {
			return false
		}
		path, e := filepath.EvalSymlinks(row.Path)
		if e != nil {
			return false
		}
		switch {
		case path == root && *row.Location == "" && !foundRoot:
			foundRoot = true
		case path == pkg && *row.Location == "node_modules/@samuelho-dev/coordinator" && row.Name != nil && *row.Name == name && !foundPackage:
			foundPackage = true
		default:
			return false
		}
	}
	return foundRoot && foundPackage
}

func main1() (failure error) {
	stage := "arguments"
	defer func() {
		if failure != nil {
			failure = stageFailure(stage, failure)
		}
	}()
	args := os.Args[1:]
	release := false
	input := ""
	for _, a := range args {
		if a == "--release" && !release {
			release = true
		} else if !strings.HasPrefix(a, "-") && input == "" {
			input = a
		} else {
			return errors.New("usage: package-smoke [--release] archive.tgz")
		}
	}
	if input == "" {
		return errors.New("archive required")
	}
	stage = "archive"
	tarball, err := filepath.Abs(input)
	if err != nil {
		return err
	}
	tarball, err = filepath.EvalSymlinks(tarball)
	if err != nil {
		return err
	}
	if err = archive(tarball, release); err != nil {
		return err
	}
	stage = "setup"
	checkout, err := os.Getwd()
	if err != nil {
		return err
	}
	root, err := os.MkdirTemp("", "coordinator-package-smoke-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(root)
	home := filepath.Join(root, "home")
	if err = os.Mkdir(home, 0700); err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(root, "package.json"), []byte(`{"private":true,"type":"module"}`), 0600); err != nil {
		return err
	}
	env := append(os.Environ(), "HOME="+home, "npm_config_cache="+filepath.Join(root, "npm-cache"))
	stage = "install"
	if _, err = run(root, env, 180*time.Second, "npm", "install", "--omit=dev", "--ignore-scripts", "--no-audit", "--no-fund", "--package-lock=false", tarball); err != nil {
		return err
	}
	stage = "manifest"
	pkg := filepath.Join(root, "node_modules", "@samuelho-dev", "coordinator")
	b, err := os.ReadFile(filepath.Join(pkg, "package.json"))
	if err != nil || len(b) > 1<<20 {
		return errors.New("invalid installed manifest")
	}
	var manifest struct {
		Name         string            `json:"name"`
		Version      string            `json:"version"`
		Bin          map[string]string `json:"bin"`
		Scripts      map[string]string `json:"scripts"`
		Dependencies map[string]any    `json:"dependencies"`
		Optional     map[string]any    `json:"optionalDependencies"`
		Peer         map[string]any    `json:"peerDependencies"`
		Pi           struct {
			Extensions []string `json:"extensions"`
		} `json:"pi"`
		Omp struct {
			Extensions []string `json:"extensions"`
		} `json:"omp"`
	}
	if json.Unmarshal(b, &manifest) != nil || manifest.Name != "@samuelho-dev/coordinator" || manifest.Version == "" || len(manifest.Bin) != 1 || strings.TrimPrefix(manifest.Bin["coordinator"], "./") != "dist/launch.cjs" || len(manifest.Dependencies)+len(manifest.Optional)+len(manifest.Peer) != 0 {
		return errors.New("invalid package manifest")
	}
	for _, s := range []string{"preinstall", "install", "postinstall"} {
		if _, ok := manifest.Scripts[s]; ok {
			return errors.New("lifecycle script forbidden")
		}
	}
	for name, extensions := range map[string][]string{"pi": manifest.Pi.Extensions, "omp": manifest.Omp.Extensions} {
		if len(extensions) != 1 || extensions[0] != "./dist/plugins/"+name+".js" {
			return errors.New("invalid extension path")
		}
	}
	for _, name := range []string{"pi.js", "omp.js"} {
		info, e := os.Stat(filepath.Join(pkg, "dist", "plugins", name))
		if e != nil || !info.Mode().IsRegular() {
			return errors.New("missing plugin")
		}
	}
	stage = "query"
	query, err := run(root, env, 30*time.Second, "npm", "query", "*", "--json")
	if err != nil {
		return err
	}
	if !validInstalled(query, root, pkg, manifest.Name) {
		return errors.New("unexpected production dependency")
	}
	stage = "version"
	binary := filepath.Join(pkg, "dist", "bin", hostTarget(), "coordinator")
	command := filepath.Join(root, "node_modules", ".bin", "coordinator")
	version, err := run(root, env, 10*time.Second, command, "--version")
	if err != nil {
		return err
	}
	direct, err := run(root, env, 10*time.Second, binary, "--version")
	if err != nil {
		return err
	}
	var a, d map[string]any
	if json.Unmarshal(version, &a) != nil || json.Unmarshal(direct, &d) != nil {
		return errors.New("invalid version output")
	}
	av, _ := json.Marshal(a)
	dv, _ := json.Marshal(d)
	if !bytes.Equal(av, dv) || a["name"] != manifest.Name || a["version"] != manifest.Version {
		return errors.New("launcher version mismatch")
	}
	if _, ok := a["sourceSha"].(string); !ok {
		return errors.New("missing source SHA")
	}
	stage = "smoke"
	env = append(env, "COORDINATOR_BINARY="+binary, "COORDINATOR_PACKAGE_ROOT="+pkg)
	if _, err = run(checkout, env, 180*time.Second, "go", "run", "./cmd/smoke"); err != nil {
		return err
	}
	fmt.Printf("{\"scenario\":\"package-acceptance\",\"status\":\"ok\",\"version\":%q,\"platform\":%q,\"arch\":%q,\"release\":%t}\n", manifest.Version, runtime.GOOS, runtime.GOARCH, release)
	return nil
}
func main() {
	if err := main1(); err != nil {
		result := map[string]string{"scenario": "package-acceptance", "status": "failed", "error": "Acceptance failed; private diagnostics withheld"}
		var stage stageError
		if errors.As(err, &stage) {
			result["stage"] = stage.stage
			if stage.detail != "" {
				result["detail"] = stage.detail
			}
		}
		b, _ := json.Marshal(result)
		fmt.Fprintln(os.Stderr, string(b))
		os.Exit(1)
	}
}
