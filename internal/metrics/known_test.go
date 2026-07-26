package metrics_test

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/ironclaw/mcp-ironclaw/internal/metrics"
)

var counterConstant = regexp.MustCompile(`Metric[A-Za-z]*\s*=\s*"([a-z0-9_]+)"`)

func countersDeclaredInSource(t *testing.T) map[string]string {
	t.Helper()

	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	declared := map[string]string{}

	err = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		source, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, match := range counterConstant.FindAllStringSubmatch(string(source), -1) {
			declared[match[1]] = strings.TrimPrefix(path, root+"/")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	return declared
}

func TestKnownCountersCoversEveryDeclaredCounter(t *testing.T) {
	declared := countersDeclaredInSource(t)
	if len(declared) == 0 {
		t.Fatal("found no counter constants in the source tree, so this test proves nothing")
	}

	known := map[string]bool{}
	for _, name := range metrics.KnownCounters() {
		known[name] = true
	}

	missing := make([]string, 0)
	for name, file := range declared {
		if !known[name] {
			missing = append(missing, name+" ("+file+")")
		}
	}
	sort.Strings(missing)

	if len(missing) > 0 {
		t.Fatalf("these counters are emitted but never pre-registered, so their dashboard panels stay empty until the counter first fires:\n  %s", strings.Join(missing, "\n  "))
	}
}

func TestKnownCountersHasNoStaleEntries(t *testing.T) {
	declared := countersDeclaredInSource(t)

	stale := make([]string, 0)
	for _, name := range metrics.KnownCounters() {
		if _, ok := declared[name]; !ok {
			stale = append(stale, name)
		}
	}
	sort.Strings(stale)

	if len(stale) > 0 {
		t.Fatalf("these counters are pre-registered but no longer emitted anywhere: %s", strings.Join(stale, ", "))
	}
}

func TestKnownCountersHasNoDuplicates(t *testing.T) {
	seen := map[string]bool{}
	for _, name := range metrics.KnownCounters() {
		if seen[name] {
			t.Fatalf("%q is listed twice", name)
		}
		seen[name] = true
	}
}
