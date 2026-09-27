package main

import (
	"os"
	"testing"
	"time"
)

func TestParseUsageSumsClusterSamples(t *testing.T) {
	stats := "" +
		"gms-stress-n1-1 10.0% 40MiB / 8GiB\n" +
		"gms-stress-n2-1 2.0% 20MiB / 8GiB\n" +
		"gms-stress-n1-1 30.0% 50MiB / 8GiB\n" +
		"gms-stress-n2-1 4.0% 22MiB / 8GiB\n"
	disk := "" +
		"gms-stress-n1-1 1000\n" +
		"gms-stress-n2-1 3000\n"
	got := parseUsage(stats, disk)
	if got.Samples != 2 {
		t.Fatalf("samples %d", got.Samples)
	}
	if got.CPUAvg != 23 || got.CPUPeak != 34 {
		t.Fatalf("cpu avg %.1f peak %.1f", got.CPUAvg, got.CPUPeak)
	}
	if got.Disk != 4000 {
		t.Fatalf("disk %d", got.Disk)
	}
	if len(got.Nodes) != 2 {
		t.Fatalf("nodes %d", len(got.Nodes))
	}
	if shortContainer(got.Nodes[0].Name) != "n1" {
		t.Fatalf("node name %s", got.Nodes[0].Name)
	}
}

func TestApplyFailoverMarkSplitsErrors(t *testing.T) {
	path := t.TempDir() + "/mark"
	kill := time.UnixMilli(1_000_000)
	ready := time.UnixMilli(1_005_000)
	body := "kill 1000000\nready 1005000\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	rep := report{}
	times := []time.Time{
		kill.Add(-time.Second),
		kill,
		kill.Add(time.Second),
		ready,
		ready.Add(time.Second),
	}
	if err := applyFailoverMark(&rep, times, path); err != nil {
		t.Fatal(err)
	}
	if !rep.Failover || rep.ElectionErrors != 2 || rep.AfterErrors != 2 {
		t.Fatalf("failover %v election %d after %d", rep.Failover, rep.ElectionErrors, rep.AfterErrors)
	}
}
