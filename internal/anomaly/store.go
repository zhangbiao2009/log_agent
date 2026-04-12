package anomaly

// BaselineStore is the persistence interface for pattern baselines.
// Implementations: MemoryStore (Phase 3), SQLiteStore (Phase 4).
// Not safe for concurrent use — AnomalyDetector serializes all access.
type BaselineStore interface {
	Get(patternID string) (PatternBaseline, bool)
	Set(patternID string, b PatternBaseline)
}

// MemoryStore is an in-memory BaselineStore.
type MemoryStore struct {
	baselines map[string]PatternBaseline
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{baselines: make(map[string]PatternBaseline)}
}

func (s *MemoryStore) Get(patternID string) (PatternBaseline, bool) {
	b, ok := s.baselines[patternID]
	return b, ok
}

func (s *MemoryStore) Set(patternID string, b PatternBaseline) {
	s.baselines[patternID] = b
}
