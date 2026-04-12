package correlator

import (
	"fmt"
	"os"
	"sort"

	"gopkg.in/yaml.v3"
)

// DependencyGraph represents directed "calls" edges between services.
type DependencyGraph struct {
	edges     map[string][]string // service -> services it calls
	reverse   map[string][]string // service -> services that call it
	component map[string]int      // service -> component ID (pre-computed)
	depth     map[string]int      // service -> BFS depth from nearest root
}

type depsFile struct {
	Services map[string]struct {
		Calls []string `yaml:"calls"`
	} `yaml:"services"`
}

// LoadFromYAML reads a dependencies.yaml file and builds the graph.
func LoadFromYAML(path string) (*DependencyGraph, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read dependencies: %w", err)
	}
	var f depsFile
	if err := yaml.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("parse dependencies: %w", err)
	}

	edges := make(map[string][]string)
	for svc, info := range f.Services {
		edges[svc] = info.Calls
	}
	return NewDependencyGraph(edges), nil
}

// NewDependencyGraph creates a graph from edge pairs.
func NewDependencyGraph(edges map[string][]string) *DependencyGraph {
	g := &DependencyGraph{
		edges:   make(map[string][]string),
		reverse: make(map[string][]string),
	}

	allServices := make(map[string]bool)
	for svc, callees := range edges {
		allServices[svc] = true
		g.edges[svc] = callees
		for _, callee := range callees {
			allServices[callee] = true
			g.reverse[callee] = append(g.reverse[callee], svc)
		}
	}

	// Pre-compute connected components (undirected BFS).
	g.component = make(map[string]int)
	compID := 0
	visited := make(map[string]bool)
	for svc := range allServices {
		if visited[svc] {
			continue
		}
		queue := []string{svc}
		visited[svc] = true
		for len(queue) > 0 {
			cur := queue[0]
			queue = queue[1:]
			g.component[cur] = compID
			for _, next := range g.edges[cur] {
				if !visited[next] {
					visited[next] = true
					queue = append(queue, next)
				}
			}
			for _, next := range g.reverse[cur] {
				if !visited[next] {
					visited[next] = true
					queue = append(queue, next)
				}
			}
		}
		compID++
	}

	// Pre-compute BFS depth from roots (services with no callers).
	g.depth = make(map[string]int)
	var roots []string
	for svc := range allServices {
		if len(g.reverse[svc]) == 0 {
			roots = append(roots, svc)
		}
	}

	visitedDepth := make(map[string]bool)
	type depthEntry struct {
		svc   string
		depth int
	}
	dQueue := make([]depthEntry, 0, len(roots))
	for _, r := range roots {
		dQueue = append(dQueue, depthEntry{r, 0})
		visitedDepth[r] = true
		g.depth[r] = 0
	}
	for len(dQueue) > 0 {
		cur := dQueue[0]
		dQueue = dQueue[1:]
		for _, next := range g.edges[cur.svc] {
			if !visitedDepth[next] {
				visitedDepth[next] = true
				g.depth[next] = cur.depth + 1
				dQueue = append(dQueue, depthEntry{next, cur.depth + 1})
			}
		}
	}

	// Services in cycles with no reachable root get depth 0.
	for svc := range allServices {
		if _, ok := g.depth[svc]; !ok {
			g.depth[svc] = 0
		}
	}

	return g
}

// Calls returns the direct downstream dependencies of svc.
func (g *DependencyGraph) Calls(svc string) []string {
	return g.edges[svc]
}

// CalledBy returns the direct upstream dependents of svc.
func (g *DependencyGraph) CalledBy(svc string) []string {
	return g.reverse[svc]
}

// Connected returns true if a and b are in the same connected component.
func (g *DependencyGraph) Connected(a, b string) bool {
	ca, aOK := g.component[a]
	cb, bOK := g.component[b]
	return aOK && bOK && ca == cb
}

// Component returns the component ID for svc, or -1 if unknown.
func (g *DependencyGraph) Component(svc string) int {
	if c, ok := g.component[svc]; ok {
		return c
	}
	return -1
}

// Depth returns the BFS distance from the nearest root (0 = root or no callers).
func (g *DependencyGraph) Depth(svc string) int {
	return g.depth[svc]
}

// ShortestPath returns the shortest directed path from `from` to `to`,
// or nil if unreachable.
func (g *DependencyGraph) ShortestPath(from, to string) []string {
	if from == to {
		return []string{from}
	}
	type entry struct {
		svc  string
		path []string
	}
	visited := map[string]bool{from: true}
	queue := []entry{{from, []string{from}}}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, next := range g.edges[cur.svc] {
			if next == to {
				return append(cur.path, to)
			}
			if !visited[next] {
				visited[next] = true
				p := make([]string, len(cur.path)+1)
				copy(p, cur.path)
				p[len(cur.path)] = next
				queue = append(queue, entry{next, p})
			}
		}
	}
	return nil
}

// Services returns all known service names sorted.
func (g *DependencyGraph) Services() []string {
	seen := make(map[string]bool)
	for svc := range g.component {
		seen[svc] = true
	}
	result := make([]string, 0, len(seen))
	for svc := range seen {
		result = append(result, svc)
	}
	sort.Strings(result)
	return result
}
