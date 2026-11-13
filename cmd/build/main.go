// Command build generates the native adapter schemas and builds distribution binaries.
// Run from the repository root: go run ./cmd/build
package main

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"

	"github.com/evanw/esbuild/pkg/api"
)

type target struct{ os, arch string }

func main() {
	if err := build(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func build() error {
	supported := map[string]target{
		"linux-x64":    {"linux", "amd64"},
		"linux-arm64":  {"linux", "arm64"},
		"darwin-x64":   {"darwin", "amd64"},
		"darwin-arm64": {"darwin", "arm64"},
	}
	arch := runtime.GOARCH
	if arch == "amd64" {
		arch = "x64"
	}
	targetList, ok := os.LookupEnv("COORDINATOR_BUILD_TARGETS")
	if !ok {
		targetList = runtime.GOOS + "-" + arch
	}
	targets := strings.Split(targetList, ",")
	for i, name := range targets {
		name = strings.TrimSpace(name)
		targets[i] = name
		t, ok := supported[name]
		if !ok {
			return fmt.Errorf("unsupported build target: %s", name)
		}
		if t.os == "darwin" && runtime.GOOS != "darwin" {
			return fmt.Errorf("Darwin builds require a macOS host with the Apple SDK and cgo")
		}
	}
	sha := os.Getenv("COORDINATOR_IMAGE_SHA")
	if sha != "" && !regexp.MustCompile(`^[a-fA-F0-9]{7,64}$`).MatchString(sha) {
		return fmt.Errorf("COORDINATOR_IMAGE_SHA must be a hexadecimal commit ID")
	}
	if err := run(nil, "run", "./cmd/generate"); err != nil {
		return err
	}
	if err := os.RemoveAll("dist"); err != nil {
		return err
	}
	if err := os.MkdirAll("dist/plugins", 0755); err != nil {
		return err
	}
	if err := os.MkdirAll("internal/integrations/assets/plugins", 0755); err != nil {
		return err
	}
	for _, harness := range []string{"pi", "omp"} {
		out := filepath.Join("dist/plugins", harness+".js")
		result := api.Build(api.BuildOptions{
			EntryPoints: []string{filepath.Join("integrations", harness+".ts")},
			Bundle:      true,
			External:    []string{"@oh-my-pi/*", "@earendil-works/*"},
			Platform:    api.PlatformNode,
			Format:      api.FormatESModule,
			Engines:     []api.Engine{{Name: api.EngineNode, Version: "22"}},
			Outfile:     out,
			Write:       true,
			LogLevel:    api.LogLevelInfo,
		})
		if len(result.Errors) != 0 {
			return fmt.Errorf("building %s adapter failed", harness)
		}
		if err := copyFile(out, filepath.Join("internal/integrations/assets/plugins", harness+".js")); err != nil {
			return err
		}
	}
	if err := copyFile("integrations/launch.cjs", "dist/launch.cjs"); err != nil {
		return err
	}
	for _, name := range targets {
		t := supported[name]
		out := filepath.Join("dist/bin", name, "coordinator")
		if err := os.MkdirAll(filepath.Dir(out), 0755); err != nil {
			return err
		}
		cgo := "0"
		if t.os == "darwin" {
			cgo = "1"
		}
		env := append(os.Environ(), "GOOS="+t.os, "GOARCH="+t.arch, "CGO_ENABLED="+cgo)
		if err := run(env, "build", "-trimpath", "-ldflags", "-X github.com/samuelho-dev/coordinator/internal/coordinator.SourceSHA="+sha, "-o", out, "./cmd/coordinator"); err != nil {
			return err
		}
	}
	return nil
}

func run(env []string, args ...string) error {
	bin, err := filepath.Abs("node_modules/.bin")
	if err != nil {
		return err
	}
	if env == nil {
		env = os.Environ()
	}
	env = append(env, "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	cmd := exec.Command("go", args...)
	cmd.Env = env
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("go %s: %w", strings.Join(args, " "), err)
	}
	return nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, in)
	closeErr := out.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}
