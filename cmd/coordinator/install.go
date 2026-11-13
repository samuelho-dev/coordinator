package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"

	"github.com/samuelho-dev/coordinator/internal/coordinator"
	"github.com/samuelho-dev/coordinator/internal/integrations"
)

func shellQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'" }
func canonicalPath(s string) bool {
	return filepath.IsAbs(s) && filepath.Clean(s) == s && !strings.ContainsAny(s, "\x00\r\n")
}
func executablePath() (string, error) {
	p, err := os.Executable()
	if err != nil {
		return "", err
	}
	return filepath.EvalSymlinks(p)
}
func trustedInstallAncestors(parent string) error {
	for path := filepath.Dir(parent); ; path = filepath.Dir(path) {
		st, err := os.Lstat(path)
		if err != nil {
			return err
		}
		owner, ok := st.Sys().(*syscall.Stat_t)
		if !st.IsDir() || !ok || (owner.Uid != uint32(os.Getuid()) && owner.Uid != 0) || (st.Mode().Perm()&0022 != 0 && st.Mode()&os.ModeSticky == 0) {
			return errors.New("untrusted ancestor")
		}
		if path == filepath.Dir(path) {
			return nil
		}
	}
}
func install(args []string) (err error) {
	f := flags("install")
	target := f.String("target", "", "")
	dest := f.String("destination", "", "")
	configFile := f.String("config", "", "")
	_ = f.Bool("source", false, "")
	dry := f.Bool("dry-run", false, "")
	messaging := f.Bool("messaging", false, "")
	if err = f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 || !canonicalPath(*dest) || (*configFile != "" && !canonicalPath(*configFile)) {
		return errors.New("invalid install path")
	}
	switch *target {
	case "pi", "omp", "claude", "codex", "hermes":
	default:
		return errors.New("invalid target")
	}
	parent := filepath.Dir(*dest)
	real, err := filepath.EvalSymlinks(parent)
	if err != nil || real != parent {
		return errors.New("invalid parent")
	}
	st, err := os.Stat(parent)
	if err != nil || !st.IsDir() || st.Mode().Perm()&0077 != 0 {
		return errors.New("private parent required")
	}
	if stat, ok := st.Sys().(*syscall.Stat_t); !ok || int(stat.Uid) != os.Getuid() || os.Getuid() == 0 {
		return errors.New("owner required")
	}
	if err = trustedInstallAncestors(parent); err != nil {
		return err
	}
	if _, err = os.Lstat(*dest); !errors.Is(err, os.ErrNotExist) {
		return errors.New("destination exists")
	}
	cfg, err := coordinator.LoadConfig(*configFile)
	if err != nil {
		return err
	}
	if *messaging && !cfg.Messaging {
		return errors.New("messaging requires connector configuration opt-in")
	}
	if _, err = coordinator.Token(cfg); err != nil {
		return err
	}
	binary, err := executablePath()
	if err != nil {
		return err
	}
	binaryData, err := os.ReadFile(binary)
	if err != nil {
		return err
	}
	arch := runtime.GOARCH
	if arch == "amd64" {
		arch = "x64"
	} else if arch == "arm64" {
		arch = "arm64"
	}
	binaryDest := filepath.Join(*dest, "dist", "bin", runtime.GOOS+"-"+arch, "coordinator")
	plugin := filepath.Join(*dest, "dist", "plugins", *target+".js")
	env := ""
	if *configFile != "" {
		env = "COORDINATOR_CONNECTOR_CONFIG=" + shellQuote(*configFile) + " "
	}
	command := func(mode string) string { return env + shellQuote(binaryDest) + " " + mode }
	files := map[string][]byte{"connector.sh": []byte("#!/bin/sh\nexec env " + command("connector") + "\n"), "mcp.sh": []byte("#!/bin/sh\nexec env " + command("mcp") + "\n"), filepath.Join("dist", "bin", runtime.GOOS+"-"+arch, "coordinator"): binaryData}
	activation := ""
	mcp := map[string]any{"command": binaryDest, "args": []string{"mcp"}}
	if *configFile != "" {
		mcp["env"] = map[string]string{"COORDINATOR_CONNECTOR_CONFIG": *configFile}
	}
	addJSON := func(path string, v any) { b, _ := json.MarshalIndent(v, "", "  "); files[path] = append(b, '\n') }
	switch *target {
	case "pi", "omp":
		b, e := integrations.Plugins.ReadFile("assets/plugins/" + *target + ".js")
		if e != nil {
			return e
		}
		files[filepath.Join("dist", "plugins", *target+".js")] = b
		activation = env + *target + " --extension " + shellQuote(plugin)
		if *target == "omp" {
			addJSON("mcp.json", map[string]any{"mcpServers": map[string]any{"coordinator": mcp}})
		}
	case "claude", "codex":
		events := []string{"SessionStart", "SessionEnd", "UserPromptSubmit", "PreToolUse", "PostToolUse", "PermissionRequest", "Stop"}
		if *target == "claude" {
			events = append(events, "PostToolUseFailure", "PostModelSwitch", "PreCompact", "StopFailure", "Elicitation", "ElicitationResult")
		} else {
			events = append(events, "Interrupt")
		}
		hooks := map[string]any{}
		for _, event := range events {
			hooks[event] = []any{map[string]any{"hooks": []any{map[string]any{"type": "command", "command": command("hook " + *target), "timeout": 3}}}}
		}
		addJSON("hooks/hooks.json", map[string]any{"hooks": hooks})
		if *target == "claude" {
			if *messaging {
				mcp["args"] = []string{"claude-channel"}
			}
			description := "Native session metadata inspector"
			if *messaging {
				description = "Native session metadata inspector and peer messaging channel"
			}
			addJSON(".claude-plugin/plugin.json", map[string]string{"name": "coordinator-inspector", "version": coordinator.Version, "description": description})
			addJSON(".mcp.json", map[string]any{"mcpServers": map[string]any{"coordinator": mcp}})
			activation = "claude --plugin-dir " + shellQuote(*dest) + "; approve the hooks and MCP tools"
			if *messaging {
				activation = "claude --plugin-dir " + shellQuote(*dest) + " --dangerously-load-development-channels server:coordinator; approve the hooks and MCP tools"
			}
		} else {
			line := fmt.Sprintf("[mcp_servers.coordinator]\ncommand = %q\nargs = [\"mcp\"]\nenv_vars = [\"COORDINATOR_API_TOKEN\", \"COORDINATOR_API_URL\", \"XDG_CONFIG_HOME\"]\n", binaryDest)
			if *configFile != "" {
				line += fmt.Sprintf("\n[mcp_servers.coordinator.env]\nCOORDINATOR_CONNECTOR_CONFIG = %q\n", *configFile)
			}
			files["mcp.toml"] = []byte(line)
			activation = "Merge generated hooks/hooks.json and mcp.toml into Codex settings; approve each hook in /hooks"
		}
	case "hermes":
		hooks := map[string]any{}
		for _, event := range []string{"on_session_start", "on_session_end", "on_session_finalize", "post_api_request"} {
			hooks[event] = []any{map[string]any{"command": "/usr/bin/env " + command("hook hermes"), "timeout": 3}}
		}
		addJSON("hooks.yaml", map[string]any{"hooks": hooks})
		addJSON("mcp.yaml", map[string]any{"mcp_servers": map[string]any{"coordinator": mcp}})
		addJSON("mcp.json", map[string]any{"mcpServers": map[string]any{"coordinator": mcp}})
		activation = "Merge generated hooks.yaml and mcp.yaml into Hermes settings; review and approve commands"
		if *messaging {
			for _, name := range []string{"plugin.yaml", "__init__.py"} {
				b, e := integrations.Hermes.ReadFile("assets/hermes/" + name)
				if e != nil {
					return e
				}
				files[filepath.Join("hermes", name)] = b
			}
			activation += "; install and enable " + filepath.Join(*dest, "hermes") + " as the coordinator native plugin, grant plugins.entries.coordinator.allow_gateway_injection: true, then use /coordinator enable in the session"
			if *configFile != "" {
				activation += "; set COORDINATOR_CONNECTOR_CONFIG=" + shellQuote(*configFile) + " in the gateway environment"
			}
		}
	}
	if !*dry {
		if err = os.Mkdir(*dest, 0700); err != nil {
			return err
		}
		defer func() {
			if err != nil {
				_ = os.RemoveAll(*dest)
			}
		}()
		for name, data := range files {
			path := filepath.Join(*dest, name)
			if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
				return err
			}
			mode := os.FileMode(0600)
			if strings.HasSuffix(name, ".sh") || path == binaryDest {
				mode = 0700
			}
			var file *os.File
			file, err = os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
			if err != nil {
				return err
			}
			_, err = file.Write(data)
			closeErr := file.Close()
			if err != nil {
				return err
			}
			if closeErr != nil {
				return closeErr
			}
		}
	}
	result := map[string]any{"destination": *dest, "binary": binaryDest, "activation": activation, "dryRun": *dry}
	if *target == "codex" && *messaging {
		result["receiver"] = false
		result["receiverReason"] = "Codex has no trusted per-tool-call thread binding; only unbound send and get are available"
	}
	if *target == "pi" || *target == "omp" {
		result["plugin"] = plugin
	}
	return output(result)
}
