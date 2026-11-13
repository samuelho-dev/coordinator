package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/samuelho-dev/coordinator/internal/coordinator"
	"github.com/samuelho-dev/coordinator/internal/wire"
)

func flags(name string) *flag.FlagSet {
	f := flag.NewFlagSet(name, flag.ContinueOnError)
	f.SetOutput(io.Discard)
	return f
}
func output(v any) error { return json.NewEncoder(os.Stdout).Encode(v) }
func noArgs(args []string) error {
	if len(args) != 0 {
		return errors.New("unexpected arguments")
	}
	return nil
}
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "coordinator: command failed; check configuration, arguments and permissions")
		os.Exit(1)
	}
}
func run(ctx context.Context, args []string) error {
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" {
		return output(map[string]any{"commands": []string{"init [--messaging]", "connect [--messaging]", "install [--messaging]", "inspector", "connector", "native", "receiver --harness hermes --session-id ID --pid PID", "claude-channel", "mcp", "list", "inspect", "message send|reply|get (JSON stdin)", "hook", "claude-statusline", "stage-config", "health"}})
	}
	mode := args[0]
	args = args[1:]
	switch mode {
	case "--version":
		if err := noArgs(args); err != nil {
			return err
		}
		return output(map[string]string{"name": "@samuelho-dev/coordinator", "version": coordinator.Version, "sourceSha": coordinator.SourceSHA})
	case "init":
		return initialize(args)
	case "connect":
		return connect(ctx, args)
	case "install":
		return install(args)
	case "stage-config":
		return stageConfig(args)
	case "health":
		return health(ctx, args)
	case "inspector":
		if err := noArgs(args); err != nil {
			return err
		}
		b, err := coordinator.ReadPrivate(os.Getenv("COORDINATOR_CONFIG"))
		if err != nil {
			return err
		}
		cfg, err := wire.Decode[wire.InspectorConfig](bytes.NewReader(b), 65536)
		if err != nil {
			return err
		}
		reg, err := coordinator.NewRegistry(cfg)
		if err != nil {
			return err
		}
		defer reg.Close()
		server := &http.Server{Addr: net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)), Handler: coordinator.Handler(reg), ReadHeaderTimeout: 10 * time.Second}
		go func() {
			<-ctx.Done()
			c, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_ = server.Shutdown(c)
		}()
		err = server.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
	cfg, err := coordinator.LoadConfig("")
	if err != nil {
		return err
	}
	switch mode {
	case "hook", "claude-statusline", "connector", "native", "receiver", "claude-channel":
		if mode == "hook" {
			if len(args) != 1 {
				return errors.New("harness required")
			}
		} else if mode != "native" && mode != "receiver" {
			if err := noArgs(args); err != nil {
				return err
			}
		}
		resolved, err := coordinator.ResolveConfig(cfg)
		if err != nil {
			return err
		}
		switch mode {
		case "hook":
			return coordinator.Capture(ctx, resolved, args[0], os.Stdin)
		case "claude-statusline":
			return coordinator.CaptureClaudeStatus(ctx, resolved, os.Stdin)
		case "connector":
			return coordinator.RunConnector(ctx, resolved)
		case "claude-channel":
			return coordinator.ServeClaudeChannel(ctx, resolved)
		case "receiver":
			f := flags(mode)
			h := f.String("harness", "", "")
			sessionID := f.String("session-id", "", "")
			pid := f.Int("pid", 0, "")
			if err := f.Parse(args); err != nil {
				return err
			}
			if f.NArg() != 0 || *h != "hermes" || *sessionID == "" || *pid <= 0 {
				return errors.New("invalid receiver flags")
			}
			return coordinator.RunReceiver(ctx, resolved, *h, *sessionID, *pid, os.Stdin, os.Stdout)
		case "native":
			f := flags(mode)
			h := f.String("harness", "", "")
			pid := f.Int("pid", 0, "")
			if err := f.Parse(args); err != nil {
				return err
			}
			if f.NArg() != 0 || (*h != "pi" && *h != "omp") || *pid <= 0 {
				return errors.New("invalid native flags")
			}
			return coordinator.RunNative(ctx, resolved, *h, *pid, os.Stdin, os.Stdout)
		}
	case "message":
		if len(args) != 1 || (args[0] != "send" && args[0] != "reply" && args[0] != "get") {
			return errors.New("message requires send, reply or get and JSON on stdin")
		}
		client, err := coordinator.NewClient(cfg)
		if err != nil {
			return err
		}
		return message(ctx, client, args[0], os.Stdin)
	case "mcp", "list", "inspect":
		if err := noArgs(args); err != nil {
			return err
		}
		client, err := coordinator.NewClient(cfg)
		if err != nil {
			return err
		}
		switch mode {
		case "mcp":
			return coordinator.ServeMCP(ctx, client)
		case "list":
			result, err := client.ListAll(ctx, wire.SessionListInput{Limit: 100})
			if err != nil {
				return err
			}
			return output(result)
		case "inspect":
			in, err := wire.Decode[wire.SessionInspectInput](os.Stdin, 65536)
			if err != nil {
				return err
			}
			result, err := client.Inspect(ctx, in.Ref)
			if err != nil {
				return err
			}
			return output(result)
		}
	}
	return errors.New("unknown command")
}

func message(ctx context.Context, client *coordinator.Client, action string, input io.Reader) error {
	switch action {
	case "send":
		in, err := wire.Decode[wire.MessageSendInput](input, 65536)
		if err != nil {
			return err
		}
		result, err := client.SendMessage(ctx, in)
		if err != nil {
			return err
		}
		return output(result)
	case "reply":
		in, err := wire.Decode[wire.MessageReplyInput](input, 65536)
		if err != nil {
			return err
		}
		if in.Caller == nil {
			return errors.New("reply requires a bound caller")
		}
		result, err := client.ReplyMessage(ctx, in)
		if err != nil {
			return err
		}
		return output(result)
	case "get":
		in, err := wire.Decode[wire.MessageGetInput](input, 65536)
		if err != nil {
			return err
		}
		result, err := client.GetMessage(ctx, in)
		if err != nil {
			return err
		}
		return output(result)
	default:
		return errors.New("invalid message action")
	}
}
