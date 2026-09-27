package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type rendered struct {
	rep    report
	use    usage
	hasUse bool
}

func renderMarkdown(path string, jsonPaths []string) error {
	reps := make([]rendered, 0, len(jsonPaths))
	for _, name := range jsonPaths {
		raw, err := os.ReadFile(name)
		if err != nil {
			return err
		}
		var rep report
		if err := json.Unmarshal(raw, &rep); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		item := rendered{rep: rep}
		statsPath := strings.TrimSuffix(name, ".json") + ".stats"
		diskPath := strings.TrimSuffix(name, ".json") + ".disk"
		statsRaw, statsErr := os.ReadFile(statsPath)
		diskRaw, diskErr := os.ReadFile(diskPath)
		if statsErr == nil || diskErr == nil {
			item.use = parseUsage(string(statsRaw), string(diskRaw))
			item.hasUse = item.use.Samples > 0 || item.use.Disk > 0
		}
		reps = append(reps, item)
	}
	return os.WriteFile(path, []byte(summaryMarkdown(reps)), 0o644)
}

func summaryMarkdown(reps []rendered) string {
	var b strings.Builder
	b.WriteString("# Stress summary\n\n")
	fmt.Fprintf(&b, "Generated %s.\n\n", time.Now().Format(time.RFC3339))
	if len(reps) == 0 {
		b.WriteString("No reports.\n")
		return b.String()
	}
	first := reps[0].rep
	fmt.Fprintf(&b, "Each target loaded %d accounts and %d notes, then ran %d clients for %.0fs with %d%% reads. ",
		first.SeedAccounts, first.SeedNotes, first.Concurrency, first.DurationSeconds, first.ReadPct)
	b.WriteString("Cluster writes go to the leader and reads go to the followers. ")
	b.WriteString("MySQL flushes the redo log and the binlog on commit. The single node fsyncs each commit. The cluster also waits for a Raft quorum. ")
	b.WriteString("TiDB reads and writes go through one SQL server to a three-node TiKV group. A commit waits for two Raft quorums. ")
	b.WriteString("The ranged target keeps the catalog on a meta group replicated to every node and places each account, with its notes, by key range: ids below the midpoint stay on the first group and the rest stay on the second. A commit waits for that group's quorum. Each node stores the catalog plus one group, so the rows are three copies spread over six nodes, with two write leaders.\n\n")
	if anyUsage(reps) {
		b.WriteString("CPU and memory are sampled about once a second while the client runs, including seed and warmup. ")
		b.WriteString("100% CPU is one core. A cluster figure is the sum of its nodes. ")
		b.WriteString("Disk is the data directory after the run: `/data` on a persist node (Badger plus the Raft log) and `/var/lib/mysql` on MySQL. ")
		b.WriteString("TiDB disk is the sum of the TiKV data directories.\n\n")
	}

	base, haveBase := mysqlBaseline(reps)
	b.WriteString("| Target | ops/s |")
	if haveBase {
		b.WriteString(" vs MySQL |")
	}
	b.WriteString(" errors | p50 | p95 | p99 | seed |\n")
	b.WriteString("| --- | ---: |")
	if haveBase {
		b.WriteString(" ---: |")
	}
	b.WriteString(" ---: | ---: | ---: | ---: | ---: |\n")
	for _, item := range reps {
		rep := item.rep
		fmt.Fprintf(&b, "| %s | %.1f |", rep.Label, rep.OpsPerSec)
		if haveBase {
			fmt.Fprintf(&b, " %s |", relativeTo(rep.OpsPerSec, base))
		}
		fmt.Fprintf(&b, " %d | %s | %s | %s | %.2fs |\n",
			rep.Errors, fmtMs(rep.P50Ms), fmtMs(rep.P95Ms), fmtMs(rep.P99Ms), rep.SeedSeconds)
	}
	b.WriteString("\n")

	if anyUsage(reps) {
		writeUsageTable(&b, reps)
	}

	for _, item := range reps {
		rep := item.rep
		fmt.Fprintf(&b, "## %s\n\n", rep.Label)
		fmt.Fprintf(&b, "Writes `%s`. Reads `%s`.\n\n", rep.Write, strings.Join(rep.Read, "`, `"))
		if rep.Failover {
			fmt.Fprintf(&b, "Election-window errors: %d. Errors after the new leader: %d.\n\n", rep.ElectionErrors, rep.AfterErrors)
		}
		if item.hasUse && len(item.use.Nodes) > 1 {
			b.WriteString("| node | CPU avg | CPU peak | mem avg | mem peak | disk |\n")
			b.WriteString("| --- | ---: | ---: | ---: | ---: | ---: |\n")
			for _, node := range item.use.Nodes {
				fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %s |\n",
					shortContainer(node.Name), fmtCPU(node.CPUAvg), fmtCPU(node.CPUPeak),
					fmtBytes(node.MemAvg), fmtBytes(float64(node.MemPeak)), fmtBytes(float64(node.Disk)))
			}
			b.WriteString("\n")
		}
		b.WriteString("| op | ops | errors | ops/s | avg | p50 | p95 | p99 | max |\n")
		b.WriteString("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
		for _, op := range rep.Ops {
			fmt.Fprintf(&b, "| %s | %d | %d | %.1f | %s | %s | %s | %s | %s |\n",
				op.Name, op.Ops, op.Errors, op.OpsSec,
				fmtMs(op.AvgMs), fmtMs(op.P50Ms), fmtMs(op.P95Ms), fmtMs(op.P99Ms), fmtMs(op.MaxMs))
		}
		for _, op := range rep.Ops {
			if op.Error != "" {
				fmt.Fprintf(&b, "\n`%s` first error: %s\n", op.Name, strings.ReplaceAll(op.Error, "\n", " "))
			}
		}
		b.WriteString("\n")
	}
	return b.String()
}

func writeUsageTable(b *strings.Builder, reps []rendered) {
	_, haveMem := mysqlResource(reps, func(u usage) float64 { return float64(u.MemPeak) })
	_, haveDisk := mysqlResource(reps, func(u usage) float64 { return float64(u.Disk) })
	_, haveCPU := mysqlResource(reps, func(u usage) float64 { return u.CPUPeak })
	b.WriteString("| Target | CPU avg | CPU peak |")
	if haveCPU {
		b.WriteString(" CPU vs MySQL |")
	}
	b.WriteString(" mem avg | mem peak |")
	if haveMem {
		b.WriteString(" mem vs MySQL |")
	}
	b.WriteString(" disk |")
	if haveDisk {
		b.WriteString(" disk vs MySQL |")
	}
	b.WriteString("\n| --- | ---: | ---: |")
	if haveCPU {
		b.WriteString(" ---: |")
	}
	b.WriteString(" ---: | ---: |")
	if haveMem {
		b.WriteString(" ---: |")
	}
	b.WriteString(" ---: |")
	if haveDisk {
		b.WriteString(" ---: |")
	}
	b.WriteString("\n")
	for _, item := range reps {
		if !item.hasUse {
			fmt.Fprintf(b, "| %s | - | - |", item.rep.Label)
			if haveCPU {
				b.WriteString(" - |")
			}
			b.WriteString(" - | - |")
			if haveMem {
				b.WriteString(" - |")
			}
			b.WriteString(" - |")
			if haveDisk {
				b.WriteString(" - |")
			}
			b.WriteString("\n")
			continue
		}
		u := item.use
		fmt.Fprintf(b, "| %s | %s | %s |", item.rep.Label, fmtCPU(u.CPUAvg), fmtCPU(u.CPUPeak))
		if haveCPU {
			base, _ := mysqlResource(reps, func(x usage) float64 { return x.CPUPeak })
			fmt.Fprintf(b, " %s |", relativeTo(u.CPUPeak, base))
		}
		fmt.Fprintf(b, " %s | %s |", fmtBytes(u.MemAvg), fmtBytes(float64(u.MemPeak)))
		if haveMem {
			base, _ := mysqlResource(reps, func(x usage) float64 { return float64(x.MemPeak) })
			fmt.Fprintf(b, " %s |", relativeTo(float64(u.MemPeak), base))
		}
		fmt.Fprintf(b, " %s |", fmtBytes(float64(u.Disk)))
		if haveDisk {
			base, _ := mysqlResource(reps, func(x usage) float64 { return float64(x.Disk) })
			fmt.Fprintf(b, " %s |", relativeTo(float64(u.Disk), base))
		}
		b.WriteString("\n")
	}
	b.WriteString("\n")
}

func anyUsage(reps []rendered) bool {
	for _, item := range reps {
		if item.hasUse {
			return true
		}
	}
	return false
}

func mysqlBaseline(reps []rendered) (float64, bool) {
	for _, item := range reps {
		if item.rep.Label == "mysql" && item.rep.OpsPerSec > 0 {
			return item.rep.OpsPerSec, true
		}
	}
	return 0, false
}

func mysqlResource(reps []rendered, pick func(usage) float64) (float64, bool) {
	for _, item := range reps {
		if item.rep.Label == "mysql" && item.hasUse {
			value := pick(item.use)
			if value > 0 {
				return value, true
			}
		}
	}
	return 0, false
}

func relativeTo(ops, base float64) string {
	if base <= 0 {
		return "-"
	}
	return fmt.Sprintf("%.2fx", ops/base)
}

type usage struct {
	Samples int
	CPUAvg  float64
	CPUPeak float64
	MemAvg  float64
	MemPeak uint64
	Disk    uint64
	Nodes   []nodeUsage
}

type nodeUsage struct {
	Name    string
	CPUAvg  float64
	CPUPeak float64
	MemAvg  float64
	MemPeak uint64
	Disk    uint64
}

type nodeAccum struct {
	name    string
	cpuSum  float64
	cpuPeak float64
	memSum  float64
	memPeak uint64
	n       int
	disk    uint64
}

func parseUsage(stats, disk string) usage {
	disks := map[string]uint64{}
	for _, line := range strings.Split(disk, "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		n, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			continue
		}
		disks[strings.TrimPrefix(fields[0], "/")] = n
	}

	var samples []map[string]struct {
		cpu float64
		mem uint64
	}
	cur := map[string]struct {
		cpu float64
		mem uint64
	}{}
	flush := func() {
		if len(cur) == 0 {
			return
		}
		samples = append(samples, cur)
		cur = map[string]struct {
			cpu float64
			mem uint64
		}{}
	}
	for _, line := range strings.Split(stats, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}
		name := strings.TrimPrefix(fields[0], "/")
		cpu, err := strconv.ParseFloat(strings.TrimSuffix(fields[1], "%"), 64)
		if err != nil {
			continue
		}
		mem, err := parseBytes(fields[2])
		if err != nil {
			continue
		}
		if _, seen := cur[name]; seen {
			flush()
		}
		cur[name] = struct {
			cpu float64
			mem uint64
		}{cpu: cpu, mem: mem}
	}
	flush()

	nodes := map[string]*nodeAccum{}
	var order []string
	seen := map[string]bool{}
	ensure := func(name string) *nodeAccum {
		if node, ok := nodes[name]; ok {
			return node
		}
		node := &nodeAccum{name: name, disk: disks[name]}
		nodes[name] = node
		if !seen[name] {
			seen[name] = true
			order = append(order, name)
		}
		return node
	}
	for _, line := range strings.Split(stats, "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 3 {
			ensure(strings.TrimPrefix(fields[0], "/"))
		}
	}
	for _, line := range strings.Split(disk, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 {
			ensure(strings.TrimPrefix(fields[0], "/"))
		}
	}

	var cpuSum, memSum float64
	var cpuPeak float64
	var memPeak uint64
	for _, sample := range samples {
		var cpu float64
		var mem uint64
		for name, point := range sample {
			cpu += point.cpu
			mem += point.mem
			node := ensure(name)
			node.n++
			node.cpuSum += point.cpu
			if point.cpu > node.cpuPeak {
				node.cpuPeak = point.cpu
			}
			node.memSum += float64(point.mem)
			if point.mem > node.memPeak {
				node.memPeak = point.mem
			}
		}
		cpuSum += cpu
		memSum += float64(mem)
		if cpu > cpuPeak {
			cpuPeak = cpu
		}
		if mem > memPeak {
			memPeak = mem
		}
	}

	var out usage
	out.Samples = len(samples)
	if len(samples) > 0 {
		out.CPUAvg = cpuSum / float64(len(samples))
		out.CPUPeak = cpuPeak
		out.MemAvg = memSum / float64(len(samples))
		out.MemPeak = memPeak
	}
	for _, name := range order {
		node := nodes[name]
		item := nodeUsage{Name: name, CPUPeak: node.cpuPeak, MemPeak: node.memPeak, Disk: node.disk}
		if node.n > 0 {
			item.CPUAvg = node.cpuSum / float64(node.n)
			item.MemAvg = node.memSum / float64(node.n)
		}
		out.Disk += node.disk
		out.Nodes = append(out.Nodes, item)
	}
	return out
}

func parseBytes(raw string) (uint64, error) {
	raw = strings.TrimSpace(raw)
	i := 0
	for i < len(raw) && (raw[i] == '.' || (raw[i] >= '0' && raw[i] <= '9')) {
		i++
	}
	if i == 0 {
		return 0, fmt.Errorf("no number in %q", raw)
	}
	num, err := strconv.ParseFloat(raw[:i], 64)
	if err != nil {
		return 0, err
	}
	unit := strings.TrimSpace(raw[i:])
	var mult float64
	switch unit {
	case "", "B":
		mult = 1
	case "KiB", "KB", "kB":
		mult = 1024
	case "MiB", "MB":
		mult = 1024 * 1024
	case "GiB", "GB":
		mult = 1024 * 1024 * 1024
	case "TiB", "TB":
		mult = 1024 * 1024 * 1024 * 1024
	default:
		return 0, fmt.Errorf("unknown unit %q", unit)
	}
	return uint64(num * mult), nil
}

func fmtCPU(pct float64) string {
	if pct <= 0 {
		return "-"
	}
	return fmt.Sprintf("%.1f%%", pct)
}

func fmtBytes(n float64) string {
	if n <= 0 {
		return "-"
	}
	units := []string{"B", "KiB", "MiB", "GiB", "TiB"}
	u := 0
	for n >= 1024 && u < len(units)-1 {
		n /= 1024
		u++
	}
	if u == 0 {
		return fmt.Sprintf("%.0f B", n)
	}
	return fmt.Sprintf("%.1f %s", n, units[u])
}

func shortContainer(name string) string {
	name = strings.TrimPrefix(name, "/")
	name = strings.TrimPrefix(name, "gms-stress-")
	name = strings.TrimSuffix(name, "-1")
	return name
}
