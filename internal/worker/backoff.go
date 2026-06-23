package worker

import (
	"math"
	"time"
)

func ExponentialBackoff(attempt int, base, max time.Duration) time.Duration {
	if attempt <= 0 {
		return base
	}
	exp := math.Pow(2, float64(attempt-1))
	d := time.Duration(float64(base) * exp)
	if d > max {
		return max
	}
	return d
}
