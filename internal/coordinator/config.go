package coordinator

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"

	"github.com/samuelho-dev/coordinator/internal/wire"
)

var Version = "0.3.0-beta.12"
var SourceSHA string

type Config struct {
	wire.ConnectorConfig
	MachineID      string
	StateDirectory string
}

var tokenPattern = regexp.MustCompile(`^[A-Za-z0-9._~+/-]+=*$`)
var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\n$`)

func newUUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	var out [36]byte
	hex.Encode(out[0:8], b[0:4])
	out[8] = '-'
	hex.Encode(out[9:13], b[4:6])
	out[13] = '-'
	hex.Encode(out[14:18], b[6:8])
	out[18] = '-'
	hex.Encode(out[19:23], b[8:10])
	out[23] = '-'
	hex.Encode(out[24:36], b[10:16])
	return string(out[:]), nil
}
func LoadConfig(file string) (wire.ConnectorConfig, error) {
	var cfg wire.ConnectorConfig
	explicit := file != ""
	if !explicit {
		file, explicit = os.LookupEnv("COORDINATOR_CONNECTOR_CONFIG")
	}
	if !explicit {
		base := os.Getenv("XDG_CONFIG_HOME")
		if base == "" {
			home, err := os.UserHomeDir()
			if err != nil {
				return cfg, err
			}
			base = filepath.Join(home, ".config")
		}
		file = filepath.Join(base, "coordinator", "inspector.json")
	}
	b, err := ReadPrivate(file)
	if err != nil && !(errors.Is(err, os.ErrNotExist) && !explicit) {
		return cfg, fmt.Errorf("read connector configuration: %w", err)
	}
	fields := map[string]json.RawMessage{}
	if err == nil {
		d := json.NewDecoder(bytes.NewReader(b))
		if err = d.Decode(&fields); err != nil || fields == nil {
			return cfg, errors.New("invalid connector configuration")
		}
		var extra any
		if d.Decode(&extra) != io.EOF {
			return cfg, errors.New("invalid connector configuration")
		}
	}
	if v, ok := os.LookupEnv("COORDINATOR_API_URL"); ok {
		fields["inspectorUrl"], _ = json.Marshal(v)
	}
	if v, ok := os.LookupEnv("COORDINATOR_API_TOKEN"); ok {
		fields["apiToken"], _ = json.Marshal(v)
	}
	if _, ok := fields["inspectorUrl"]; !ok && !explicit && len(b) == 0 {
		return cfg, nil
	}
	encoded, e := json.Marshal(fields)
	if e != nil {
		return cfg, errors.New("invalid connector configuration")
	}
	cfg, e = wire.Decode[wire.ConnectorConfig](bytes.NewReader(encoded), maxPrivateBytes)
	if e != nil {
		return cfg, errors.New("invalid connector configuration")
	}
	return cfg, nil
}
func Token(cfg wire.ConnectorConfig) (string, error) {
	if cfg.APIToken != "" {
		return cfg.APIToken, nil
	}
	if cfg.TokenFile == "" {
		return "", errors.New("connector credential required")
	}
	b, err := ReadPrivate(cfg.TokenFile)
	if err != nil {
		return "", errors.New("cannot read connector credential")
	}
	s := strings.TrimSpace(string(b))
	if len(s) < 16 || len(s) > 4096 || !tokenPattern.MatchString(s) {
		return "", errors.New("invalid connector credential")
	}
	return s, nil
}
func ResolveConfig(cfg wire.ConnectorConfig) (Config, error) {
	result := Config{ConnectorConfig: cfg}
	base := os.Getenv("XDG_STATE_HOME")
	if base == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return result, err
		}
		if runtime.GOOS == "darwin" {
			base = filepath.Join(home, "Library", "Application Support")
		} else {
			base = filepath.Join(home, ".local", "state")
		}
	}
	if !canonical(base) {
		return result, errors.New("unsafe state path")
	}
	dir := filepath.Join(base, "coordinator")
	if err := WritePrivate(dir, map[string][]byte{}); err != nil {
		return result, err
	}
	result.StateDirectory = dir
	target := filepath.Join(dir, "machine-id")
	b, err := readPrivate(target, true)
	if errors.Is(err, os.ErrNotExist) {
		id, e := newUUID()
		if e != nil {
			return result, e
		}
		temp, e := newUUID()
		if e != nil {
			return result, e
		}
		tmp := filepath.Join(dir, ".machine-id-"+temp)
		if e = WritePrivate(dir, map[string][]byte{tmp: []byte(id + "\n")}); e != nil {
			return result, e
		}
		defer os.Remove(tmp)
		if e = os.Link(tmp, target); e != nil && !errors.Is(e, os.ErrExist) {
			return result, e
		}
		b, err = readPrivate(target, true)
	}
	if err != nil {
		return result, err
	}
	if !uuidPattern.Match(b) {
		return result, errors.New("invalid installation identity")
	}
	result.MachineID = string(b[:len(b)-1])
	return result, nil
}
