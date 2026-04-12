package notify

import (
	"fmt"
	"testing"
)

func TestAnomalyKind_String(t *testing.T) {
	tests := []struct {
		kind AnomalyKind
		want string
	}{
		{AnomalyNone, "none"},
		{AnomalyNewPattern, "new_pattern"},
		{AnomalySpike, "spike"},
		{AnomalyRateJump, "rate_jump"},
		{AnomalyKind(99), fmt.Sprintf("unknown(%d)", 99)},
	}
	for _, tc := range tests {
		got := tc.kind.String()
		if got != tc.want {
			t.Errorf("AnomalyKind(%d).String() = %q, want %q", int(tc.kind), got, tc.want)
		}
	}
}
