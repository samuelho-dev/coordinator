package observe

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/samuelho-dev/coordinator/internal/wire"
)

type ProcessRow struct {
	PID                                  int
	PPID                                 int
	StartIdentity                        string
	Ownership                            string
	CPUPercent                           *float64
	CPUTimeMs                            *float64
	RSSBytes                             *float64
	SampleStartedAt, SampledAt, WindowMs float64
	UnavailableReason                    string
}
type ProcessSample struct {
	Processes                            []ProcessRow
	SampleStartedAt, SampledAt, WindowMs float64
	Partial, Truncated                   bool
}
type counter struct {
	pid, ppid int
	birth     string
	cpu, rss  float64
	at        time.Time
}

type limitedBuffer struct {
	bytes.Buffer
	limit     int
	truncated bool
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	n := len(p)
	if n > b.limit-b.Len() {
		b.truncated = true
		p = p[:b.limit-b.Len()]
	}
	_, _ = b.Buffer.Write(p)
	return n, nil
}
func nowMS() float64 { return float64(time.Now().UnixNano()) / 1e6 }
func nativeCommand(ctx context.Context, path string, args ...string) ([]byte, error) {
	b, truncated, err := nativeCommandCapture(ctx, path, args...)
	if err == nil && truncated {
		err = errors.New("native output limit")
	}
	return b, err
}
func nativeCommandCapture(ctx context.Context, path string, args ...string) ([]byte, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, path, args...)
	cmd.Env = []string{"LC_ALL=C", "LANG=C", "PATH=/usr/bin:/bin:/usr/sbin:/sbin"}
	out := &limitedBuffer{limit: 1024 * 1024}
	cmd.Stdout = out
	err := cmd.Run()
	return out.Bytes(), out.truncated, err
}
func linuxBoot() (string, error) {
	b, e := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if e != nil {
		return "", e
	}
	s := strings.TrimSpace(string(b))
	if len(s) != 36 {
		return "", errors.New("invalid boot identity")
	}
	return s, nil
}
func linuxCounter(pid int, boot string, hz float64) (counter, error) {
	b, e := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if e != nil {
		return counter{}, e
	}
	s := string(b)
	end := strings.LastIndexByte(s, ')')
	if end < 0 {
		return counter{}, errors.New("invalid proc stat")
	}
	f := strings.Fields(s[end+1:])
	if len(f) < 22 {
		return counter{}, errors.New("short proc stat")
	}
	p, e := strconv.Atoi(f[1])
	if e != nil {
		return counter{}, e
	}
	u, e := strconv.ParseUint(f[11], 10, 64)
	if e != nil {
		return counter{}, e
	}
	v, e := strconv.ParseUint(f[12], 10, 64)
	if e != nil {
		return counter{}, e
	}
	birth, e := strconv.ParseUint(f[19], 10, 64)
	if e != nil {
		return counter{}, e
	}
	rss, e := strconv.ParseInt(f[21], 10, 64)
	if e != nil || rss < 0 {
		return counter{}, errors.New("invalid rss")
	}
	return counter{pid, p, fmt.Sprintf("linux:%s:%d", boot, birth), float64(u+v) * 1000 / hz, float64(rss) * float64(os.Getpagesize()), time.Now()}, nil
}
func ticks(ctx context.Context) (float64, error) {
	p, e := exec.LookPath("getconf")
	if e != nil {
		return 0, e
	}
	p, e = filepath.Abs(p)
	if e != nil {
		return 0, e
	}
	b, e := nativeCommand(ctx, p, "CLK_TCK")
	if e != nil {
		return 0, e
	}
	n, e := strconv.ParseFloat(strings.TrimSpace(string(b)), 64)
	if e != nil || n <= 0 {
		return 0, errors.New("invalid CLK_TCK")
	}
	return n, nil
}
func parseDarwinSnapshot(b []byte, truncated bool, at time.Time) (map[int]counter, error) {
	if truncated {
		if i := bytes.LastIndexByte(b, '\n'); i >= 0 {
			b = b[:i+1]
		} else {
			b = nil
		}
	}
	out := make(map[int]counter)
	sc := bufio.NewScanner(bytes.NewReader(b))
	for sc.Scan() {
		f := strings.Fields(sc.Text())
		if len(f) != 9 {
			return nil, errors.New("invalid ps row")
		}
		pid, e := strconv.Atoi(f[0])
		if e != nil {
			return nil, e
		}
		ppid, e := strconv.Atoi(f[1])
		if e != nil {
			return nil, e
		}
		cpu, e := parsePSTime(f[7])
		if e != nil {
			return nil, e
		}
		rss, e := strconv.ParseFloat(f[8], 64)
		if e != nil {
			return nil, e
		}
		out[pid] = counter{pid, ppid, "darwin:" + strings.Join(f[2:7], " "), cpu, rss * 1024, at}
	}
	return out, sc.Err()
}
func darwinSnapshot(ctx context.Context, roots []wire.ProcessIdentity) (map[int]counter, bool, bool, error) {
	b, truncated, e := nativeCommandCapture(ctx, "/bin/ps", "-axo", "pid=,ppid=,lstart=,time=,rss=")
	if e != nil {
		return nil, false, false, e
	}
	all, e := parseDarwinSnapshot(b, truncated, time.Now())
	if e != nil {
		return nil, false, false, e
	}
	m := make(map[int]counter)
	partial := false
	for _, r := range roots {
		c, ok := all[r.PID]
		if !ok {
			b, e := nativeCommand(ctx, "/bin/ps", "-p", strconv.Itoa(r.PID), "-o", "pid=,ppid=,lstart=,time=,rss=")
			if e != nil {
				partial = true
				continue
			}
			targeted, err := parseDarwinSnapshot(b, false, time.Now())
			if err != nil {
				partial = true
				continue
			}
			c, ok = targeted[r.PID]
		}
		if ok {
			m[r.PID] = c
		} else {
			partial = true
		}
	}
	for pid, c := range all {
		if _, ok := m[pid]; ok {
			continue
		}
		if len(m) >= 4096 {
			truncated = true
			break
		}
		m[pid] = c
	}
	return m, truncated, partial, nil
}
func parsePSTime(s string) (float64, error) {
	days := 0.
	if x := strings.SplitN(s, "-", 2); len(x) == 2 {
		var e error
		days, e = strconv.ParseFloat(x[0], 64)
		if e != nil {
			return 0, e
		}
		s = x[1]
	}
	f := strings.Split(s, ":")
	if len(f) < 2 || len(f) > 3 {
		return 0, errors.New("invalid ps time")
	}
	n := 0.
	for _, v := range f {
		x, e := strconv.ParseFloat(v, 64)
		if e != nil {
			return 0, e
		}
		n = n*60 + x
	}
	return (n + days*86400) * 1000, nil
}
func Identify(ctx context.Context, pid int) (wire.ProcessIdentity, error) {
	if pid <= 0 {
		return wire.ProcessIdentity{}, errors.New("invalid pid")
	}
	var birth string
	var e error
	if runtime.GOOS == "linux" {
		var boot string
		boot, e = linuxBoot()
		if e == nil {
			var c counter
			c, e = linuxCounter(pid, boot, 1)
			birth = c.birth
		}
	} else if runtime.GOOS == "darwin" {
		var b []byte
		b, e = nativeCommand(ctx, "/bin/ps", "-p", strconv.Itoa(pid), "-o", "pid=,ppid=,lstart=")
		if e == nil {
			f := strings.Fields(string(b))
			if len(f) != 7 || f[0] != strconv.Itoa(pid) {
				e = errors.New("invalid ps identity")
			} else {
				birth = "darwin:" + strings.Join(f[2:7], " ")
			}
		}
	} else {
		e = errors.New("unsupported platform")
	}
	if e != nil {
		return wire.ProcessIdentity{}, e
	}
	return wire.ProcessIdentity{PID: pid, StartIdentity: birth, Ownership: "unknown"}, nil
}
func snapshot(ctx context.Context, roots []wire.ProcessIdentity) (map[int]counter, bool, bool, error) {
	if runtime.GOOS == "darwin" {
		return darwinSnapshot(ctx, roots)
	}
	if runtime.GOOS != "linux" {
		return nil, false, false, errors.New("unsupported platform")
	}
	boot, e := linuxBoot()
	if e != nil {
		return nil, false, false, e
	}
	hz, e := ticks(ctx)
	if e != nil {
		return nil, false, false, e
	}
	dir, e := os.Open("/proc")
	if e != nil {
		return nil, false, false, e
	}
	defer dir.Close()
	pids := make(map[int]bool)
	for _, r := range roots {
		pids[r.PID] = true
	}
	truncated := false
	for {
		entries, err := dir.ReadDir(256)
		for _, entry := range entries {
			pid, e := strconv.Atoi(entry.Name())
			if e == nil && pid > 0 {
				if len(pids) >= 4096 && !pids[pid] {
					truncated = true
					break
				}
				pids[pid] = true
			}
		}
		if truncated || err != nil {
			if err != nil && err != io.EOF {
				return nil, false, false, err
			}
			break
		}
	}
	m := make(map[int]counter, len(pids))
	partial := false
	for pid := range pids {
		if c, e := linuxCounter(pid, boot, hz); e == nil {
			m[pid] = c
		} else {
			partial = true
		}
	}
	return m, truncated, partial, nil
}

type processKey struct {
	pid   int
	birth string
}

// attributeProcesses only charges identities that remain stable across both snapshots.
func attributeProcesses(roots []wire.ProcessIdentity, valid map[processKey]bool, a, b map[int]counter) ([]ProcessRow, bool) {
	claims := make(map[processKey]string)
	partial := false
	for _, r := range roots {
		key := processKey{r.PID, r.StartIdentity}
		if !valid[key] {
			partial = true
			continue
		}
		owner := r.Ownership
		if owner != "owned" && owner != "shared" {
			owner = "unknown"
		}
		if previous, ok := claims[key]; ok {
			if previous == "unknown" || owner == "unknown" {
				owner = "unknown"
			} else {
				owner = "shared"
			}
		}
		claims[key] = owner
	}
	rows := make([]ProcessRow, 0)
	seen := make(map[int]int)
	generation := 0
	visit := func(c counter, source, other map[int]counter) {
		generation++
		cur := c
		owner := ""
		unstable := false
		for seen[cur.pid] != generation {
			seen[cur.pid] = generation
			if root, ok := claims[processKey{cur.pid, cur.birth}]; ok {
				if owner != "" || root == "shared" {
					owner = "shared"
				} else if root == "unknown" {
					owner = "unknown"
				} else {
					owner = "owned"
				}
			}
			prev, ok := other[cur.pid]
			if cur.pid != c.pid && (!ok || prev.birth != cur.birth || prev.ppid != cur.ppid) {
				unstable = true
			}
			next, ok := source[cur.ppid]
			if !ok {
				break
			}
			cur = next
		}
		if owner == "" {
			return
		}
		row := ProcessRow{PID: c.pid, PPID: c.ppid, StartIdentity: c.birth, Ownership: owner}
		first, ok1 := a[c.pid]
		last, ok2 := b[c.pid]
		if ok1 && first.birth == c.birth {
			row.SampleStartedAt = float64(first.at.UnixNano()) / 1e6
		}
		if ok2 && last.birth == c.birth {
			row.SampledAt = float64(last.at.UnixNano()) / 1e6
		}
		if ok1 && ok2 && first.birth == c.birth && last.birth == c.birth {
			row.WindowMs = float64(last.at.Sub(first.at)) / float64(time.Millisecond)
		}
		if unstable || !ok1 || !ok2 || first.birth != last.birth || first.ppid != last.ppid || last.cpu < first.cpu || (runtime.GOOS == "darwin" && ((first.cpu == 0 && first.rss == 0) || (last.cpu == 0 && last.rss == 0))) {
			row.Ownership = "unknown"
			row.UnavailableReason = "process identity or counters changed or unavailable"
			partial = true
		} else {
			delta := last.at.Sub(first.at).Seconds()
			if delta > 0 {
				value := (last.cpu - first.cpu) / 10 / delta
				row.CPUPercent = &value
			}
			cpu, rss := last.cpu, last.rss
			row.CPUTimeMs = &cpu
			row.RSSBytes = &rss
		}
		rows = append(rows, row)
	}
	for _, c := range b {
		visit(c, b, a)
	}
	for pid, c := range a {
		if _, ok := b[pid]; !ok {
			visit(c, a, b)
		}
	}
	for _, r := range roots {
		found := false
		for _, row := range rows {
			if row.PID == r.PID && row.StartIdentity == r.StartIdentity {
				found = true
				break
			}
		}
		if !found {
			partial = true
			rows = append(rows, ProcessRow{PID: r.PID, StartIdentity: r.StartIdentity, Ownership: "unknown", UnavailableReason: "root-exited-or-identity-changed"})
		}
	}
	return rows, partial
}
func (s *Sampler) Sample(ctx context.Context, roots []wire.ProcessIdentity) (ProcessSample, error) {
	result := ProcessSample{SampleStartedAt: nowMS(), Processes: []ProcessRow{}}
	if len(roots) == 0 {
		result.SampledAt = nowMS()
		return result, nil
	}
	if len(roots) > 4096 {
		return result, errors.New("too many roots")
	}
	for _, r := range roots {
		if r.PID <= 0 || r.StartIdentity == "" {
			return result, errors.New("invalid root")
		}
	}
	valid := make(map[processKey]bool)
	for _, r := range roots {
		key := processKey{r.PID, r.StartIdentity}
		id, err := Identify(ctx, r.PID)
		ok := err == nil && id.StartIdentity == r.StartIdentity
		if previous, exists := valid[key]; exists {
			ok = ok && previous
		}
		valid[key] = ok
	}
	a, ta, pa, e := snapshot(ctx, roots)
	if e != nil {
		return result, e
	}
	timer := time.NewTimer(200 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return result, ctx.Err()
	case <-timer.C:
	}
	b, tb, pb, e := snapshot(ctx, roots)
	if e != nil {
		return result, e
	}
	for _, r := range roots {
		key := processKey{r.PID, r.StartIdentity}
		id, err := Identify(ctx, r.PID)
		valid[key] = valid[key] && err == nil && id.StartIdentity == r.StartIdentity
	}
	result.SampledAt = nowMS()
	result.WindowMs = result.SampledAt - result.SampleStartedAt
	result.Truncated = ta || tb
	var partial bool
	result.Processes, partial = attributeProcesses(roots, valid, a, b)
	result.Partial = result.Truncated || pa || pb || partial
	return result, nil
}
