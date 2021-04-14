package geb

import (
	"testing"
	"time"
)

func TestMaxElapsedTime(t *testing.T) {
	maxElapsedTime := 1 * time.Second
	startTime := time.Now()
	b := NewExponentialBackOff(maxElapsedTime, DefaultMaxInterval, DefaultRandomizationFactor)

	for {
		hasNext, duration := b.NextBackOff()
		if !hasNext {
			if !time.Now().After(startTime.Add(maxElapsedTime)) {
				t.Errorf("wanted elapsed time to be greater than: %s; got: %s", maxElapsedTime.String(), time.Since(startTime).String())
			}
			break
		}
		time.Sleep(duration)
	}
}

func TestBackoffOverFlow(t *testing.T) {
	maxElapsedTime := 1 * time.Second
	maxInterval := 100 * time.Millisecond
	maxDuration := maxInterval + time.Duration(int64(float64(maxInterval.Nanoseconds())*DefaultRandomizationFactor))
	b := NewExponentialBackOff(maxElapsedTime, maxInterval, DefaultRandomizationFactor)

	for {
		hasNext, duration := b.NextBackOff()
		if duration > maxDuration {
			t.Errorf("max interval: %s; current interval: %s", maxInterval.String(), duration.String())
			break
		}
		if !hasNext {
			break
		}
		time.Sleep(duration)
	}
}
