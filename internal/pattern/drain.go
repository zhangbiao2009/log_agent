package pattern

import (
	"fmt"
	"hash/fnv"
	"strings"
	"time"
)

// DrainConfig controls Drain's behavior.
type DrainConfig struct {
	Depth               int     // fixed tree depth (default: 4)
	SimilarityThreshold float64 // merge threshold (default: 0.5)
	MaxChildren         int     // max children per tree node (default: 100)
	MaxPatterns         int     // max total patterns to track (default: 10000)
}

func (c *DrainConfig) setDefaults() {
	if c.Depth == 0 {
		c.Depth = 4
	}
	if c.SimilarityThreshold == 0 {
		c.SimilarityThreshold = 0.5
	}
	if c.MaxChildren == 0 {
		c.MaxChildren = 100
	}
	if c.MaxPatterns == 0 {
		c.MaxPatterns = 10000
	}
}

// Pattern represents a templatized log pattern discovered by Drain.
type Pattern struct {
	ID          string    // hex-encoded fnv1a hash of the template string
	Template    string    // human-readable, e.g. "connection timeout to <*>:<*>"
	Tokens      []string  // tokenized template; variable positions are "<*>"
	TokenCount  int       // number of tokens (used for tree indexing)
	LastMatched time.Time // updated on every match (for LRU eviction)
}

// node is an internal Drain tree node. Leaf nodes hold pattern clusters.
type node struct {
	children map[string]*node // key: token value or "<*>" for wildcard
	clusters []*Pattern       // only populated at leaf level
}

func newNode() *node {
	return &node{children: make(map[string]*node)}
}

// Drain is an online log parser that discovers and merges log templates.
// It is NOT safe for concurrent use — the caller must serialize access.
type Drain struct {
	config   DrainConfig
	root     *node
	patterns map[string]*Pattern // ID → Pattern
}

// NewDrain creates a new Drain instance with the given config.
func NewDrain(cfg DrainConfig) *Drain {
	cfg.setDefaults()
	return &Drain{
		config:   cfg,
		root:     newNode(),
		patterns: make(map[string]*Pattern),
	}
}

// Process takes a preprocessed (non-JSON) log string, matches or creates a
// pattern, and returns the matching Pattern with LastMatched updated.
// Thread-unsafe — the caller must serialize calls.
func (d *Drain) Process(preprocessed string) *Pattern {
	tokens := strings.Fields(preprocessed)
	if len(tokens) == 0 {
		tokens = []string{"<EMPTY>"}
	}

	leaf := d.getLeaf(tokens)
	pat := d.matchCluster(leaf, tokens)
	if pat == nil {
		pat = d.createPattern(tokens)
		leaf.clusters = append(leaf.clusters, pat)
		d.patterns[pat.ID] = pat
		if len(d.patterns) > d.config.MaxPatterns {
			d.evictLRU(pat.ID)
		}
	}
	pat.LastMatched = time.Now()
	return pat
}

// Patterns returns a snapshot of all currently tracked patterns.
func (d *Drain) Patterns() map[string]*Pattern {
	out := make(map[string]*Pattern, len(d.patterns))
	for k, v := range d.patterns {
		out[k] = v
	}
	return out
}

// getLeaf navigates (creating nodes as needed) from root to the leaf for tokens.
func (d *Drain) getLeaf(tokens []string) *node {
	// Level 1: token count
	countKey := fmt.Sprintf("%d", len(tokens))
	cur := d.root
	cur = getOrCreate(cur, countKey)

	// Levels 2..depth-1: use first tokens as routing keys.
	for depth := 1; depth < d.config.Depth-1; depth++ {
		if depth > len(tokens) {
			break
		}
		tok := tokens[depth-1]
		// If the token is already a wildcard from preprocessing, route to <*>.
		if isWildcard(tok) {
			tok = "<*>"
		}
		// If too many children, collapse to wildcard.
		if _, exists := cur.children[tok]; !exists && len(cur.children) >= d.config.MaxChildren {
			tok = "<*>"
		}
		cur = getOrCreate(cur, tok)
	}
	return cur
}

// matchCluster finds the best-matching cluster in the leaf, merges if similar
// enough, and returns it. Returns nil if no match.
func (d *Drain) matchCluster(leaf *node, tokens []string) *Pattern {
	var bestPat *Pattern
	bestSim := -1.0

	for _, pat := range leaf.clusters {
		sim := similarity(pat.Tokens, tokens)
		if sim > bestSim {
			bestSim = sim
			bestPat = pat
		}
	}

	if bestPat == nil || bestSim < d.config.SimilarityThreshold {
		return nil
	}

	// Merge: replace differing positions with <*>.
	oldID := bestPat.ID
	merged := merge(bestPat.Tokens, tokens)
	if !tokenSliceEqual(merged, bestPat.Tokens) {
		// Template changed — update ID.
		delete(d.patterns, oldID)
		bestPat.Tokens = merged
		bestPat.Template = strings.Join(merged, " ")
		bestPat.ID = patternID(bestPat.Template)
		d.patterns[bestPat.ID] = bestPat
	}
	return bestPat
}

// createPattern builds a new Pattern from a token sequence.
func (d *Drain) createPattern(tokens []string) *Pattern {
	toks := make([]string, len(tokens))
	copy(toks, tokens)
	tmpl := strings.Join(toks, " ")
	return &Pattern{
		ID:         patternID(tmpl),
		Template:   tmpl,
		Tokens:     toks,
		TokenCount: len(toks),
	}
}

// evictLRU removes the least-recently-matched pattern (O(n) scan).
// skipID is exempted from eviction (the pattern just created).
func (d *Drain) evictLRU(skipID string) {
	var oldest *Pattern
	for _, p := range d.patterns {
		if p.ID == skipID {
			continue
		}
		if oldest == nil || p.LastMatched.Before(oldest.LastMatched) {
			oldest = p
		}
	}
	if oldest != nil {
		delete(d.patterns, oldest.ID)
		// Also remove from leaf clusters (best-effort linear scan).
		d.removeFromClusters(d.root, oldest)
	}
}

func (d *Drain) removeFromClusters(n *node, pat *Pattern) {
	for i, c := range n.clusters {
		if c.ID == pat.ID {
			n.clusters = append(n.clusters[:i], n.clusters[i+1:]...)
			return
		}
	}
	for _, child := range n.children {
		d.removeFromClusters(child, pat)
	}
}

// --- helpers ---

func getOrCreate(n *node, key string) *node {
	if child, ok := n.children[key]; ok {
		return child
	}
	child := newNode()
	n.children[key] = child
	return child
}

// similarity computes the fraction of matching tokens between a template and
// an input token sequence. Wildcards in the template match any token.
func similarity(templateTokens, inputTokens []string) float64 {
	if len(templateTokens) != len(inputTokens) {
		return 0
	}
	if len(templateTokens) == 0 {
		return 1
	}
	match := 0
	for i, t := range templateTokens {
		if t == "<*>" || t == inputTokens[i] {
			match++
		}
	}
	return float64(match) / float64(len(templateTokens))
}

// merge returns a new token slice where positions that differ become "<*>".
func merge(templateTokens, inputTokens []string) []string {
	out := make([]string, len(templateTokens))
	for i, t := range templateTokens {
		if t == "<*>" || t == inputTokens[i] {
			out[i] = t
		} else {
			out[i] = "<*>"
		}
	}
	return out
}

// isWildcard returns true for tokens that are already wildcards from preprocessing.
func isWildcard(tok string) bool {
	return tok == "<*>" || tok == "<IP>" || tok == "<UUID>" || tok == "<HEX>" || tok == "<NUM>" || tok == "<EMPTY>"
}

func tokenSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// patternID computes a stable hex ID from a template string using fnv1a-64.
func patternID(template string) string {
	h := fnv.New64a()
	h.Write([]byte(template))
	return fmt.Sprintf("%016x", h.Sum64())
}
