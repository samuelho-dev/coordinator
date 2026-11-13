//go:build linux

package observe

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type boundary struct{ dir, mount, identity string }
type mountRow struct{ root, mount, device, kind, controllers string }

func file(path string) (string, bool) {
	f, e := os.Open(path)
	if e != nil {
		return "", false
	}
	defer f.Close()
	b, e := io.ReadAll(io.LimitReader(f, 1048577))
	if e != nil || len(b) > 1048576 {
		return "", false
	}
	return strings.TrimSpace(string(b)), true
}
func number(s string) *float64 {
	v, e := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if e != nil || v < 0 {
		return nil
	}
	return ptr(v)
}
func fields(s string) map[string]float64 {
	out := map[string]float64{}
	for _, line := range strings.Split(s, "\n") {
		f := strings.Fields(strings.ReplaceAll(line, ":", " "))
		if len(f) > 1 {
			if n := number(f[1]); n != nil {
				out[f[0]] = *n
			}
		}
	}
	return out
}
func minValue(vs ...*float64) *float64 {
	var out *float64
	for _, v := range vs {
		if v != nil && (out == nil || *v < *out) {
			out = ptr(*v)
		}
	}
	return out
}
func cpuSet(s string) *float64 {
	if s == "" {
		return nil
	}
	last := -1
	count := 0
	for _, part := range strings.Split(s, ",") {
		f := strings.Split(part, "-")
		if len(f) > 2 {
			return nil
		}
		a, e := strconv.Atoi(f[0])
		if e != nil || a <= last || a < 0 {
			return nil
		}
		b := a
		if len(f) == 2 {
			b, e = strconv.Atoi(f[1])
			if e != nil || b < a {
				return nil
			}
		}
		count += b - a + 1
		last = b
	}
	if count <= 0 {
		return nil
	}
	return ptr(float64(count))
}
func unmount(s string) string {
	r := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`)
	return r.Replace(s)
}
func chain(b *boundary) []string {
	if b == nil {
		return nil
	}
	var result []string
	for p := b.dir; len(result) < 128; p = filepath.Dir(p) {
		result = append(result, p)
		if p == b.mount {
			return result
		}
		if p == filepath.Dir(p) {
			break
		}
	}
	return nil
}
func containsCSV(s, item string) bool {
	for _, x := range strings.Split(s, ",") {
		if x == item {
			return true
		}
	}
	return false
}
func readNativeMachine(ctx context.Context, pid int) machineNative {
	proc := fmt.Sprintf("/proc/%d", pid)
	membership, ok := file(proc + "/cgroup")
	mounts, ok2 := file(proc + "/mountinfo")
	boot, _ := file("/proc/sys/kernel/random/boot_id")
	if !ok || !ok2 {
		return blank("linux-scope-unavailable")
	}
	type group struct{ id, controllers, member string }
	var groups []group
	for _, line := range strings.Split(membership, "\n") {
		if line == "" {
			continue
		}
		f := strings.SplitN(line, ":", 3)
		if len(f) != 3 || !strings.HasPrefix(f[2], "/") {
			return blank("linux-scope-unavailable")
		}
		groups = append(groups, group{f[0], f[1], f[2]})
	}
	var rows []mountRow
	for _, line := range strings.Split(mounts, "\n") {
		parts := strings.SplitN(line, " - ", 2)
		if len(parts) != 2 {
			continue
		}
		l, r := strings.Fields(parts[0]), strings.Fields(parts[1])
		if len(l) < 5 || len(r) < 3 || (r[0] != "cgroup" && r[0] != "cgroup2") {
			continue
		}
		root, mount := unmount(l[3]), unmount(l[4])
		if strings.HasPrefix(root, "/") && strings.HasPrefix(mount, "/") {
			rows = append(rows, mountRow{root, filepath.Join(proc, "root", mount), l[2], r[0], r[2]})
		}
	}
	sort.Slice(rows, func(i, j int) bool { return len(rows[i].root) > len(rows[j].root) })
	resolve := func(controller string, v2 bool) *boundary {
		var member string
		for _, g := range groups {
			if (v2 && g.id == "0" && g.controllers == "") || (!v2 && containsCSV(g.controllers, controller)) {
				member = g.member
				break
			}
		}
		if member == "" {
			return nil
		}
		for _, c := range strings.Split(member, "/") {
			if c == ".." || c == "." {
				return nil
			}
		}
		for _, r := range rows {
			if (v2 && r.kind != "cgroup2") || (!v2 && (r.kind != "cgroup" || !containsCSV(r.controllers, controller))) {
				continue
			}
			rooted := r.root == "/" || member == r.root || strings.HasPrefix(member, r.root+"/")
			rel := member
			if rooted {
				rel = strings.TrimPrefix(member, r.root)
			}
			dir := filepath.Join(r.mount, strings.TrimPrefix(rel, "/"))
			if dir != r.mount && !strings.HasPrefix(dir, r.mount+"/") {
				continue
			}
			if !rooted {
				p, ok := file(filepath.Join(dir, "cgroup.procs"))
				found := false
				if ok {
					for _, x := range strings.Fields(p) {
						if x == strconv.Itoa(pid) {
							found = true
						}
					}
				}
				if !found {
					continue
				}
			}
			info, e := os.Stat(dir)
			if e != nil {
				continue
			}
			st, ok := info.Sys().(*syscall.Stat_t)
			if !ok {
				continue
			}
			return &boundary{dir, r.mount, fmt.Sprintf("%s:%s:%s:%d", r.device, r.root, member, st.Ino)}
		}
		return nil
	}
	v2 := false
	has := func(c string) bool {
		for _, g := range groups {
			if containsCSV(g.controllers, c) && g.controllers != "" {
				return true
			}
		}
		return false
	}
	for _, g := range groups {
		if g.id == "0" && g.controllers == "" {
			v2 = true
		}
	}
	if len(groups) == 0 {
		if len(rows) > 0 {
			return blank("linux-scope-unavailable")
		}
		n := linuxHost(boot)
		m, ok := file(proc + "/cgroup")
		mi, ok2 := file(proc + "/mountinfo")
		b, _ := file("/proc/sys/kernel/random/boot_id")
		if !ok || !ok2 || m != membership || mi != mounts || b != boot {
			return blank("linux-scope-changed")
		}
		return n
	}
	cpuV2, memV2, setV2 := v2 && !has("cpuacct"), v2 && !has("memory"), v2 && !has("cpuset")
	cpu := resolve("cpuacct", cpuV2)
	quota := cpu
	if !cpuV2 {
		quota = resolve("cpu", false)
	}
	memory := resolve("memory", memV2)
	cpuset := resolve("cpuset", setV2)
	source := "linux-cgroup-v1"
	if v2 {
		source = "linux-cgroup-v2"
		if len(groups) > 1 {
			source = "linux-cgroup-hybrid"
		}
	}
	n := blank(source)
	n.scope = "cgroup"
	n.cpuDefinition = "Cgroup cumulative execution / monotonic elapsed; visible ancestor quota/cpuset bounds, hidden ancestors unknown"
	n.memoryDefinition = "Cgroup accounted charges including descendants; visible hierarchical hard limits; headroom is not available memory"
	ns, e := os.Readlink(proc + "/ns/cgroup")
	if boot != "" && e == nil && cpu != nil && memory != nil {
		q, c := "<nil>", "<nil>"
		if quota != nil {
			q = quota.identity
		}
		if cpuset != nil {
			c = cpuset.identity
		}
		n.identity = fmt.Sprintf("%s:%s:%s:%s:%s:%s", boot, ns, cpu.identity, memory.identity, q, c)
	}
	if cpu != nil {
		if cpuV2 {
			s, _ := file(filepath.Join(cpu.dir, "cpu.stat"))
			if v, ok := fields(s)["usage_usec"]; ok {
				n.usage = ptr(v * 1000)
			}
		} else {
			s, _ := file(filepath.Join(cpu.dir, "cpuacct.usage"))
			n.usage = number(s)
		}
	}
	n.at = time.Now()
	unknown := quota == nil
	var capacity *float64
	for _, dir := range chain(quota) {
		if cpuV2 {
			s, ok := file(filepath.Join(dir, "cpu.max"))
			if !ok && dir == quota.mount && os.IsNotExist(fileErr(filepath.Join(dir, "cpu.max"))) {
				continue
			}
			f := strings.Fields(s)
			if !ok || len(f) != 2 {
				unknown = true
				continue
			}
			p := number(f[1])
			if p == nil || *p <= 0 {
				unknown = true
				continue
			}
			if f[0] != "max" {
				q := number(f[0])
				if q == nil {
					unknown = true
				} else {
					capacity = minValue(capacity, ptr(*q / *p))
				}
			}
		} else {
			q, _ := file(filepath.Join(dir, "cpu.cfs_quota_us"))
			ptext, _ := file(filepath.Join(dir, "cpu.cfs_period_us"))
			p := number(ptext)
			if p == nil || *p <= 0 {
				unknown = true
				continue
			}
			if q != "-1" {
				v := number(q)
				if v == nil {
					unknown = true
				} else {
					capacity = minValue(capacity, ptr(*v / *p))
				}
			}
		}
	}
	if quota != nil && chain(quota) == nil {
		unknown = true
	}
	var set string
	setOK := false
	if cpuset != nil {
		name := "cpuset.effective_cpus"
		if setV2 {
			name = "cpuset.cpus.effective"
		}
		set, setOK = file(filepath.Join(cpuset.dir, name))
		if !setV2 && !setOK && os.IsNotExist(fileErr(filepath.Join(cpuset.dir, name))) {
			for _, dir := range chain(cpuset) {
				set, setOK = file(filepath.Join(dir, "cpuset.cpus"))
				if !setOK || set != "" {
					break
				}
			}
		}
	}
	setCount := cpuSet(set)
	if (cpuset == nil && (setV2 || has("cpuset"))) || (cpuset != nil && (!setOK || setCount == nil)) {
		unknown = true
	}
	online, _ := file("/sys/devices/system/cpu/online")
	capacity = minValue(capacity, setCount, cpuSet(online))
	if !unknown {
		n.capacity = capacity
	}
	n.topology = fmt.Sprintf("%s:%s:%v", online, set, n.capacityValue())
	if memory != nil {
		name := "memory.usage_in_bytes"
		if memV2 {
			name = "memory.current"
		}
		s, _ := file(filepath.Join(memory.dir, name))
		n.used = number(s)
		memUnknown := false
		var limit *float64
		ch := chain(memory)
		if ch == nil {
			memUnknown = true
		}
		for _, dir := range ch {
			if !memV2 && dir != memory.dir {
				h, ok := file(filepath.Join(dir, "memory.use_hierarchy"))
				if !ok || (h != "0" && h != "1") {
					memUnknown = true
					continue
				}
				if h == "0" {
					continue
				}
			}
			name = "memory.limit_in_bytes"
			if memV2 {
				name = "memory.max"
			}
			path := filepath.Join(dir, name)
			text, ok := file(path)
			if memV2 && ((!ok && dir == memory.mount && os.IsNotExist(fileErr(path))) || text == "max") {
				continue
			}
			val := number(text)
			if !memV2 && val != nil && *val >= 0x7fffffff00000000 {
				continue
			}
			if !ok || val == nil || *val <= 0 {
				memUnknown = true
			} else {
				limit = minValue(limit, val)
			}
		}
		if !memV2 && len(ch) > 1 {
			s, _ := file(filepath.Join(memory.dir, "memory.stat"))
			if v, ok := fields(s)["hierarchical_memory_limit"]; ok && v < 0x7fffffff00000000 {
				limit = minValue(limit, ptr(v))
			}
		}
		mem, _ := file("/proc/meminfo")
		if v, ok := fields(mem)["MemTotal"]; ok {
			limit = minValue(limit, ptr(v*1024))
		}
		if !memUnknown {
			n.memory = limit
		}
	}
	m2, ok := file(proc + "/cgroup")
	mounts2, mountOK := file(proc + "/mountinfo")
	ns2, nsErr := os.Readlink(proc + "/ns/cgroup")
	boot2, _ := file("/proc/sys/kernel/random/boot_id")
	if !ok || !mountOK || m2 != membership || mounts2 != mounts || nsErr != nil || ns2 != ns || boot2 != boot {
		return blank("linux-scope-changed")
	}
	n.partial = n.identity == "" || n.usage == nil || n.capacity == nil || n.used == nil || n.memory == nil
	return n
}
func fileErr(path string) error { _, err := os.Stat(path); return err }
func (n machineNative) capacityValue() string {
	if n.capacity == nil {
		return "unknown"
	}
	return strconv.FormatFloat(*n.capacity, 'g', -1, 64)
}
func linuxHost(boot string) machineNative {
	n := blank("linux-proc")
	n.scope = "host"
	if boot != "" {
		n.identity = "linux:" + boot
	}
	stat, _ := file("/proc/stat")
	count := 0
	var topology []string
	for _, line := range strings.Split(stat, "\n") {
		f := strings.Fields(line)
		if len(f) == 0 {
			continue
		}
		if f[0] == "cpu" && len(f) >= 9 {
			v := make([]float64, 8)
			valid := true
			for i := range v {
				x := number(f[i+1])
				if x == nil {
					valid = false
					break
				}
				v[i] = *x
			}
			if valid {
				n.usage = ptr(v[0] + v[1] + v[2] + v[5] + v[6])
				sum := float64(0)
				for _, x := range v {
					sum += x
				}
				n.total = ptr(sum)
			}
		} else if strings.HasPrefix(f[0], "cpu") {
			if _, e := strconv.Atoi(strings.TrimPrefix(f[0], "cpu")); e == nil {
				count++
				topology = append(topology, f[0])
			}
		}
	}
	n.at = time.Now()
	if count > 0 {
		n.capacity = ptr(float64(count))
	}
	online, _ := file("/sys/devices/system/cpu/online")
	n.topology = strings.Join(topology, ",") + ":" + online
	mem, _ := file("/proc/meminfo")
	f := fields(mem)
	if v, ok := f["MemTotal"]; ok {
		n.memory = ptr(v * 1024)
	}
	if v, ok := f["MemAvailable"]; ok {
		n.available = ptr(v * 1024)
	}
	if n.memory != nil && n.available != nil && *n.available <= *n.memory {
		n.used = ptr(*n.memory - *n.available)
	} else {
		n.available = nil
	}
	n.cpuDefinition = "Execution user+nice+system+irq+softirq / all non-guest CPU ticks"
	n.memoryDefinition = "MemTotal minus MemAvailable; available is kernel MemAvailable, never MemFree"
	n.partial = n.usage == nil || n.used == nil || n.identity == ""
	return n
}
