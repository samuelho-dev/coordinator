package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/samuelho-dev/coordinator/internal/coordinator"
	"github.com/samuelho-dev/coordinator/internal/wire"
)

func stageConfig(args []string) error {
	f := flags("stage-config")
	input := f.String("input", "/input", "")
	dest := f.String("destination", "/config/private", "")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 || !canonicalPath(*input) || !canonicalPath(*dest) {
		return errors.New("invalid paths")
	}
	files := map[string][]byte{}
	for _, name := range []string{"inspector.json", "health.token"} {
		p := filepath.Join(*input, name)
		resolved, err := filepath.EvalSymlinks(p)
		if err != nil || !strings.HasPrefix(resolved, *input+string(os.PathSeparator)) {
			return errors.New("invalid secret")
		}
		st, err := os.Stat(resolved)
		if err != nil || !st.Mode().IsRegular() || st.Size() > 65536 {
			return errors.New("invalid secret")
		}
		b, err := os.ReadFile(resolved)
		if err != nil {
			return err
		}
		files[name] = b
	}
	cfg, err := wire.Decode[wire.InspectorConfig](bytes.NewReader(files["inspector.json"]), 65536)
	if err != nil {
		return err
	}
	token := strings.TrimSpace(string(files["health.token"]))
	if token == "" || strings.ContainsAny(token, "\r\n") {
		return errors.New("invalid health token")
	}
	sum := sha256.Sum256([]byte(token))
	valid := false
	for _, principal := range cfg.Principals {
		if principal.TokenHash == hex.EncodeToString(sum[:]) {
			valid = true
		}
	}
	if !valid {
		return errors.New("health token not authorized")
	}
	return coordinator.WritePrivate(*dest, files)
}
func health(ctx context.Context, args []string) error {
	f := flags("health")
	url := f.String("url", "http://127.0.0.1:8787", "")
	file := f.String("token-file", "/config/private/health.token", "")
	source := f.String("source-sha", os.Getenv("COORDINATOR_EXPECTED_SOURCE_SHA"), "")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 || !canonicalPath(*file) {
		return errors.New("invalid flags")
	}
	b, err := coordinator.ReadPrivate(*file)
	if err != nil {
		return err
	}
	token := strings.TrimSpace(string(b))
	if token == "" || strings.ContainsAny(token, "\r\n") || *source == "" {
		return errors.New("invalid health configuration")
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(*url, "/")+"/readyz", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	client := http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 65536))
	if res.StatusCode < 200 || res.StatusCode >= 300 || res.Header.Get("x-coordinator-source-sha") != *source {
		return errors.New("unhealthy")
	}
	return nil
}
