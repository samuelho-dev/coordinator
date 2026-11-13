//go:build darwin

package observe

/*
#include <mach/mach.h>
#include <stdint.h>
static int cpu_ticks(uint64_t *busy, uint64_t *total) {
 host_cpu_load_info_data_t info;
 mach_msg_type_number_t count = HOST_CPU_LOAD_INFO_COUNT;
 mach_port_t host = mach_host_self();
 kern_return_t rc = host_statistics(host, HOST_CPU_LOAD_INFO, (host_info_t)&info, &count);
 mach_port_deallocate(mach_task_self(), host);
 if (rc != KERN_SUCCESS) return 0;
 *busy = (uint64_t)info.cpu_ticks[CPU_STATE_USER] + info.cpu_ticks[CPU_STATE_SYSTEM] + info.cpu_ticks[CPU_STATE_NICE];
 *total = *busy + info.cpu_ticks[CPU_STATE_IDLE];
 return 1;
}
*/
import "C"

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func readNativeMachine(ctx context.Context, pid int) machineNative {
	n := blank("darwin")
	n.scope = "host"
	command := func(path string, args ...string) string {
		b, e := nativeCommand(ctx, path, args...)
		if e != nil || len(b) > 65536 {
			return ""
		}
		return strings.TrimSpace(string(b))
	}
	boot := command("/usr/sbin/sysctl", "-n", "kern.boottime")
	re := regexp.MustCompile(`^\{\s*sec\s*=\s*(\d+)\s*,\s*usec\s*=\s*(\d+)\s*\}`)
	match := re.FindStringSubmatch(boot)
	host, e := os.Hostname()
	if len(match) == 3 && e == nil && host != "" {
		sec, e1 := strconv.ParseUint(match[1], 10, 64)
		usec, e2 := strconv.ParseUint(match[2], 10, 64)
		if e1 == nil && e2 == nil && usec < 1000000 {
			n.identity = fmt.Sprintf("darwin:%s:%d:%d", host, sec, usec)
		}
	}
	var busy, total C.uint64_t
	if C.cpu_ticks(&busy, &total) != 0 {
		n.usage = ptr(float64(busy))
		n.total = ptr(float64(total))
	}
	n.at = time.Now()
	parse := func(s string) *float64 {
		v, e := strconv.ParseUint(strings.TrimSpace(s), 10, 53)
		if e != nil {
			return nil
		}
		return ptr(float64(v))
	}
	n.capacity = parse(command("/usr/sbin/sysctl", "-n", "hw.logicalcpu"))
	if n.capacity != nil && *n.capacity <= 0 {
		n.capacity = nil
	}
	if n.capacity != nil {
		n.topology = fmt.Sprint(*n.capacity)
	}
	n.memory = parse(command("/usr/sbin/sysctl", "-n", "hw.memsize"))
	vm := command("/usr/bin/vm_stat")
	pageMatch := regexp.MustCompile(`page size of (\d+) bytes`).FindStringSubmatch(vm)
	var page *float64
	if len(pageMatch) == 2 {
		page = parse(pageMatch[1])
	}
	counts := map[string]float64{}
	for _, line := range strings.Split(vm, "\n") {
		p := strings.SplitN(line, ":", 2)
		if len(p) == 2 {
			v := parse(strings.TrimSuffix(strings.TrimSpace(p[1]), "."))
			if v != nil {
				counts[p[0]] = *v
			}
		}
	}
	active, a := counts["Pages active"]
	inactive, b := counts["Pages inactive"]
	wired, c := counts["Pages wired down"]
	compressed, d := counts["Pages occupied by compressor"]
	if page != nil && *page > 0 && a && b && c && d {
		used := (active + inactive + wired + compressed) * *page
		if used <= 9007199254740991 {
			n.used = ptr(used)
		}
	}
	n.cpuDefinition = "Mach aggregate user+nice+system execution / all CPU ticks"
	n.memoryDefinition = "vm_stat active + inactive + wired + compressor-occupied pages times actual page size; available undefined"
	n.partial = n.identity == "" || n.usage == nil || n.capacity == nil || n.used == nil || n.memory == nil
	return n
}
