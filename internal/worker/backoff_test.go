package worker

import (
	"testing"
	"time"
)

func TestExponentialBackoff(t *testing.T) {
	base := time.Second
	max := 30 * time.Second

	tests := []struct {
		attempt  int
		expected time.Duration
	}{
		{0, time.Second},
		{1, time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{4, 8 * time.Second},
		{10, 30 * time.Second},
	}

	for _, tt := range tests {
		got := ExponentialBackoff(tt.attempt, base, max)
		if got != tt.expected {
			t.Errorf("attempt %d: got %v, want %v", tt.attempt, got, tt.expected)
		}
	}
}
