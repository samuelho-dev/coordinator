package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/samuelho-dev/coordinator/internal/coordinator"
	"github.com/samuelho-dev/coordinator/internal/wire"
	"golang.org/x/term"
)

func configHome() string {
	if p := os.Getenv("XDG_CONFIG_HOME"); p != "" {
		return filepath.Join(p, "coordinator")
	}
	h, _ := os.UserHomeDir()
	return filepath.Join(h, ".config", "coordinator")
}
func jsonBytes(v any) []byte { b, _ := json.MarshalIndent(v, "", "  "); return append(b, '\n') }
func initialize(args []string) error {
	f := flags("init")
	dir := f.String("directory", filepath.Join(configHome(), "server"), "")
	owner := f.String("owner", "owner", "")
	host := f.String("listen-host", "127.0.0.1", "")
	port := f.Int("port", 8787, "")
	messaging := f.Bool("messaging", false, "")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 || (*host != "localhost" && net.ParseIP(*host) == nil) || *port < 1 || *port > 65535 || !regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,255}$`).MatchString(*owner) {
		return errors.New("invalid setup")
	}
	var random [32]byte
	if _, err := rand.Read(random[:]); err != nil {
		return err
	}
	token := base64.RawURLEncoding.EncodeToString(random[:])
	hash := sha256.Sum256([]byte(token))
	cfg := wire.InspectorConfig{Host: *host, Port: *port, Principals: []wire.InspectorPrincipal{{ID: *owner, Owner: *owner, TokenHash: hex.EncodeToString(hash[:]), Messaging: *messaging}}}
	if err := coordinator.WritePrivate(*dir, map[string][]byte{"server.json": jsonBytes(cfg), "owner.token": []byte(token + "\n")}); err != nil {
		return err
	}
	return output(map[string]string{"configFile": filepath.Join(*dir, "server.json"), "tokenFile": filepath.Join(*dir, "owner.token")})
}
func connect(ctx context.Context, args []string) error {
	f := flags("connect")
	endpoint := f.String("endpoint", "", "")
	tokenFile := f.String("token-file", "", "")
	messaging := f.Bool("messaging", false, "")
	def := os.Getenv("COORDINATOR_CONNECTOR_CONFIG")
	if def == "" {
		def = filepath.Join(configHome(), "inspector.json")
	}
	configFile := f.String("config", def, "")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 || *endpoint == "" {
		return errors.New("endpoint required")
	}
	token := os.Getenv("COORDINATOR_API_TOKEN")
	if *tokenFile != "" {
		b, err := coordinator.ReadPrivate(*tokenFile)
		if err != nil {
			return err
		}
		token = strings.TrimSpace(string(b))
	} else if token == "" {
		fmt.Fprint(os.Stderr, "Owner token: ")
		b, err := term.ReadPassword(int(os.Stdin.Fd()))
		fmt.Fprintln(os.Stderr)
		if err != nil {
			return err
		}
		token = string(b)
	}
	candidate := wire.ConnectorConfig{InspectorURL: *endpoint, APIToken: token, Messaging: *messaging}
	raw := jsonBytes(candidate)
	if _, err := wire.Decode[wire.ConnectorConfig](bytes.NewReader(raw), 65536); err != nil {
		return err
	}
	client, err := coordinator.NewClient(candidate)
	if err != nil {
		return err
	}
	if _, err = client.List(ctx, wire.SessionListInput{Limit: 1}); err != nil {
		return err
	}
	files := map[string][]byte{}
	if *tokenFile == "" {
		*tokenFile = filepath.Join(filepath.Dir(*configFile), "inspector.token")
		files[*tokenFile] = []byte(token + "\n")
	}
	files[*configFile] = jsonBytes(wire.ConnectorConfig{InspectorURL: *endpoint, TokenFile: *tokenFile, Messaging: *messaging})
	if err := coordinator.WritePrivate(filepath.Dir(*configFile), files); err != nil {
		return err
	}
	return output(map[string]string{"configFile": *configFile, "tokenFile": *tokenFile})
}
