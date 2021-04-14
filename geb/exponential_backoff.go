package geb

import (
	"math/rand"
	"time"
)

const (
	DefaultMaxElapsedTime      = 1 * time.Minute
	DefaultMaxInterval         = 5 * time.Second
	DefaultRandomizationFactor = 0.5

	initialInterval = 50 * time.Millisecond
	multiplier      = 1.5
)

type ExponentialBackoff struct {
	currentInterval     time.Duration
	maxElapsedTime      time.Duration
	maxInterval         time.Duration
	randomizationFactor float64
	startTime           time.Time
}

func NewExponentialBackOff(maxElapsedTime, maxInterval time.Duration, randomizationFactor float64) *ExponentialBackoff {
	return &ExponentialBackoff{
		currentInterval:     initialInterval,
		maxElapsedTime:      maxElapsedTime,
		maxInterval:         maxInterval,
		randomizationFactor: randomizationFactor,
		startTime:           time.Now(),
	}
}

func (b *ExponentialBackoff) NextBackOff() (bool, time.Duration) {
	if time.Now().After(b.startTime.Add(b.maxElapsedTime)) {
		return false, 0
	}
	floatInterval := float64(b.currentInterval)
	if floatInterval >= float64(b.maxInterval)/multiplier {
		b.currentInterval = b.maxInterval
	} else {
		b.currentInterval = time.Duration(floatInterval * multiplier)
	}
	return true, time.Duration(floatInterval * (1 + rand.Float64()*b.randomizationFactor)) //nolint:gosec
}
