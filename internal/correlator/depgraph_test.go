package correlator

import (
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// --- 3.1 Loading ---

func TestDepGraph_LoadFromYAML(t *testing.T) {
	yaml := `services:
  order-service:
    calls: [payment-service, inventory-service]
  payment-service:
    calls: [bank-gateway]
  inventory-service:
    calls: [warehouse-db]
`
	path := writeTempYAML(t, yaml)
	g, err := LoadFromYAML(path)
	if err != nil {
		t.Fatalf("LoadFromYAML: %v", err)
	}
	calls := g.Calls("order-service")
	sort.Strings(calls)
	if len(calls) != 2 || calls[0] != "inventory-service" || calls[1] != "payment-service" {
		t.Errorf("Calls(order-service) = %v, want [inventory-service payment-service]", calls)
	}
}

func TestDepGraph_LoadEmptyFile(t *testing.T) {
	yaml := `services: {}`
	path := writeTempYAML(t, yaml)
	g, err := LoadFromYAML(path)
	if err != nil {
		t.Fatalf("LoadFromYAML: %v", err)
	}
	if len(g.Services()) != 0 {
		t.Errorf("expected empty graph, got %d services", len(g.Services()))
	}
}

func TestDepGraph_LoadUnknownServiceReference(t *testing.T) {
	yaml := `services:
  order-service:
    calls: [missing-service]
`
	path := writeTempYAML(t, yaml)
	g, err := LoadFromYAML(path)
	if err != nil {
		t.Fatalf("LoadFromYAML: %v", err)
	}
	calls := g.Calls("order-service")
	if len(calls) != 1 || calls[0] != "missing-service" {
		t.Errorf("Calls(order-service) = %v, want [missing-service]", calls)
	}
	if leaf := g.Calls("missing-service"); len(leaf) != 0 {
		t.Errorf("Calls(missing-service) = %v, want []", leaf)
	}
}

// --- 3.2 Calls / CalledBy ---

func testGraph_ABCD() *DependencyGraph {
	return NewDependencyGraph(map[string][]string{
		"A": {"B", "C"},
		"B": {"D"},
	})
}

func TestDepGraph_Calls(t *testing.T) {
	g := testGraph_ABCD()
	calls := g.Calls("A")
	sort.Strings(calls)
	if len(calls) != 2 || calls[0] != "B" || calls[1] != "C" {
		t.Errorf("Calls(A) = %v, want [B C]", calls)
	}
	if d := g.Calls("D"); len(d) != 0 {
		t.Errorf("Calls(D) = %v, want []", d)
	}
}

func TestDepGraph_CalledBy(t *testing.T) {
	g := testGraph_ABCD()
	cb := g.CalledBy("B")
	if len(cb) != 1 || cb[0] != "A" {
		t.Errorf("CalledBy(B) = %v, want [A]", cb)
	}
	if a := g.CalledBy("A"); len(a) != 0 {
		t.Errorf("CalledBy(A) = %v, want []", a)
	}
}

func TestDepGraph_CalledByUnknownService(t *testing.T) {
	g := testGraph_ABCD()
	if cb := g.CalledBy("nonexistent"); len(cb) != 0 {
		t.Errorf("CalledBy(nonexistent) = %v, want []", cb)
	}
}

// --- 3.3 Connected ---

func TestDepGraph_ConnectedDirectEdge(t *testing.T) {
	g := NewDependencyGraph(map[string][]string{"A": {"B"}})
	if !g.Connected("A", "B") {
		t.Error("Connected(A,B) = false, want true")
	}
	if !g.Connected("B", "A") {
		t.Error("Connected(B,A) = false, want true (bidirectional)")
	}
}

func TestDepGraph_ConnectedTransitive(t *testing.T) {
	g := NewDependencyGraph(map[string][]string{"A": {"B"}, "B": {"C"}})
	if !g.Connected("A", "C") {
		t.Error("Connected(A,C) = false, want true (transitive)")
	}
}

func TestDepGraph_NotConnectedDisjoint(t *testing.T) {
	g := NewDependencyGraph(map[string][]string{
		"A": {"B"},
		"C": {"D"},
	})
	if g.Connected("A", "D") {
		t.Error("Connected(A,D) = true, want false (disjoint components)")
	}
}

func TestDepGraph_ConnectedSelf(t *testing.T) {
	g := NewDependencyGraph(map[string][]string{"A": {"B"}})
	if !g.Connected("A", "A") {
		t.Error("Connected(A,A) = false, want true")
	}
}

// --- 3.4 Depth ---

func TestDepGraph_DepthLinearChain(t *testing.T) {
	g := NewDependencyGraph(map[string][]string{
		"A": {"B"},
		"B": {"C"},
		"C": {"D"},
	})
	want := map[string]int{"A": 0, "B": 1, "C": 2, "D": 3}
	for svc, exp := range want {
		if got := g.Depth(svc); got != exp {
			t.Errorf("Depth(%s) = %d, want %d", svc, got, exp)
		}
	}
}

func TestDepGraph_DepthDiamond(t *testing.T) {
	g := NewDependencyGraph(map[string][]string{
		"A": {"B", "C"},
		"B": {"D"},
		"C": {"D"},
	})
	if got := g.Depth("D"); got != 2 {
		t.Errorf("Depth(D) = %d, want 2", got)
	}
}

func TestDepGraph_DepthIsolatedService(t *testing.T) {
	g := NewDependencyGraph(map[string][]string{
		"A": {"B"},
		"X": {},
	})
	if got := g.Depth("X"); got != 0 {
		t.Errorf("Depth(X) = %d, want 0", got)
	}
}

// --- 3.5 ShortestPath ---

func TestDepGraph_ShortestPathExists(t *testing.T) {
	g := NewDependencyGraph(map[string][]string{
		"A": {"B"},
		"B": {"C"},
	})
	path := g.ShortestPath("A", "C")
	if len(path) != 3 || path[0] != "A" || path[1] != "B" || path[2] != "C" {
		t.Errorf("ShortestPath(A,C) = %v, want [A B C]", path)
	}
}

func TestDepGraph_ShortestPathUnreachable(t *testing.T) {
	g := NewDependencyGraph(map[string][]string{
		"A": {"B"},
		"C": {"D"},
	})
	path := g.ShortestPath("A", "D")
	if path != nil {
		t.Errorf("ShortestPath(A,D) = %v, want nil", path)
	}
}

// --- helpers ---

func writeTempYAML(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "deps.yaml")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write temp yaml: %v", err)
	}
	return path
}
