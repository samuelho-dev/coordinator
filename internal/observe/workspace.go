package observe

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/samuelho-dev/coordinator/internal/wire"
)

func Workspace(ctx context.Context, cwd string, previous *wire.SessionWorkspace, project *wire.SessionProject) wire.SessionWorkspace {
	result := wire.SessionWorkspace{CWD: cwd}
	now := nowMS()
	if project != nil {
		root, e := filepath.EvalSymlinks(project.Root)
		if e == nil {
			current, e := filepath.EvalSymlinks(cwd)
			if e == nil {
				ri, re := os.Stat(root)
				ci, ce := os.Stat(current)
				rel, e := filepath.Rel(root, current)
				if re == nil && ce == nil && ri.IsDir() && ci.IsDir() && e == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)) && !filepath.IsAbs(rel) {
					copy := *project
					copy.Root = root
					result.Project = &copy
				}
			}
		}
	}
	if previous != nil && previous.CWD == cwd && previous.Git != nil && now >= previous.Git.ObservedAt && now-previous.Git.ObservedAt < 30000 {
		result.Git = previous.Git
		return result
	}
	git := &wire.SessionGit{State: "unavailable", ObservedAt: now}
	result.Git = git
	exe, e := exec.LookPath("git")
	if e != nil {
		return result
	}
	exe, e = filepath.Abs(exe)
	if e != nil {
		return result
	}
	ctx, cancel := context.WithTimeout(ctx, 1900*time.Millisecond)
	defer cancel()
	run := func(args ...string) (string, error) {
		cmd := exec.CommandContext(ctx, exe, args...)
		cmd.Dir = cwd
		cmd.Env = []string{"PATH=/usr/bin:/bin", "LC_ALL=C", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_OPTIONAL_LOCKS=0", "GIT_TERMINAL_PROMPT=0"}
		out := &limitedBuffer{limit: 16384}
		cmd.Stdout = out
		err := cmd.Run()
		return out.String(), err
	}
	root, e := run("rev-parse", "--show-toplevel")
	if e != nil {
		var exit *exec.ExitError
		if errors.As(e, &exit) && exit.ExitCode() == 128 && outsideRepository(cwd) {
			git.State = "not-repository"
		}
		return result
	}
	if !strings.HasSuffix(root, "\n") {
		return result
	}
	root, e = filepath.EvalSymlinks(strings.TrimSuffix(root, "\n"))
	if e != nil {
		return result
	}
	common, e := run("rev-parse", "--path-format=absolute", "--git-common-dir")
	if e != nil || !strings.HasSuffix(common, "\n") {
		return result
	}
	common = strings.TrimSuffix(common, "\n")
	if !filepath.IsAbs(common) {
		return result
	}
	common, e = filepath.EvalSymlinks(common)
	if e != nil {
		return result
	}
	branchText, e := run("symbolic-ref", "--quiet", "HEAD")
	var branch *string
	if e == nil {
		if !strings.HasPrefix(branchText, "refs/heads/") || !strings.HasSuffix(branchText, "\n") {
			return result
		}
		value := strings.TrimSuffix(strings.TrimPrefix(branchText, "refs/heads/"), "\n")
		branch = &value
	} else {
		var exit *exec.ExitError
		if !errors.As(e, &exit) || exit.ExitCode() != 1 {
			return result
		}
	}
	headText, e := run("rev-parse", "--verify", "--quiet", "HEAD")
	var head *string
	if e == nil {
		if !strings.HasSuffix(headText, "\n") {
			return result
		}
		value := strings.TrimSuffix(headText, "\n")
		if len(value) != 40 && len(value) != 64 {
			return result
		}
		head = &value
	} else {
		var exit *exec.ExitError
		if branch == nil || !errors.As(e, &exit) || exit.ExitCode() != 1 {
			return result
		}
	}
	git.State = "available"
	git.WorktreeRoot = root
	git.CommonDir = common
	git.Branch = branch
	git.Head = head
	return result
}
func outsideRepository(cwd string) bool {
	dir, e := filepath.EvalSymlinks(cwd)
	if e != nil {
		return false
	}
	info, e := os.Stat(dir)
	if e != nil || !info.IsDir() {
		return false
	}
	for {
		if _, e = os.Lstat(filepath.Join(dir, ".git")); e == nil || !os.IsNotExist(e) {
			return false
		}
		head, eh := os.Stat(filepath.Join(dir, "HEAD"))
		objects, eo := os.Stat(filepath.Join(dir, "objects"))
		refs, er := os.Stat(filepath.Join(dir, "refs"))
		if eh == nil && eo == nil && er == nil && !head.IsDir() && objects.IsDir() && refs.IsDir() {
			return false
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return true
		}
		dir = parent
	}
}
