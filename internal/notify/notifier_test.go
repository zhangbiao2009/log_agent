package notify

import (
	"fmt"
	"github.com/zhangbiao2009/log_agent/internal/core"
	"testing"
)

func TestAnomalyKind_String(t *testing.T) {
	tests := []struct {
		kind core.AnomalyKind
		want string
	}{
		{core.AnomalyNone, "none"},
		{core.AnomalyNewPattern, "new_pattern"},
		{core.AnomalySpike, "spike"},
		{core.AnomalyRateJump, "rate_jump"},
		{core.AnomalyKind(99), fmt.Sprintf("unknown(%d)", 99)},
	}
	for _, tc := range tests {
		got := tc.kind.String()
		if got != tc.want {
			t.Errorf("core.AnomalyKind(%d).String() = %q, want %q", int(tc.kind), got, tc.want)
		}
	}
}
