package test

import (
	"fmt"
	"sync"
)

type Counter struct {
	triedPublishes int
	publishes      map[uint32]struct{}
	consumes       map[uint32]struct{}
	m              *sync.Mutex
}

func NewCounter(triedPublishes int) *Counter {
	return &Counter{
		triedPublishes: triedPublishes,
		publishes:      make(map[uint32]struct{}, triedPublishes),
		consumes:       make(map[uint32]struct{}, triedPublishes),
		m:              &sync.Mutex{},
	}
}

func (c *Counter) TriedPublishes() int {
	return c.triedPublishes
}

func (c *Counter) AddPublish(id uint32) {
	c.m.Lock()
	defer c.m.Unlock()

	c.publishes[id] = struct{}{}
}

func (c *Counter) PublishCount() int {
	c.m.Lock()
	defer c.m.Unlock()

	return len(c.publishes)
}

func (c *Counter) AddConsume(id uint32) {
	c.m.Lock()
	defer c.m.Unlock()

	c.consumes[id] = struct{}{}
}

func (c *Counter) ConsumeCount() int {
	c.m.Lock()
	defer c.m.Unlock()

	return len(c.consumes)
}

func (c *Counter) PublishedNotConsumed() int {
	c.m.Lock()
	defer c.m.Unlock()

	var count int
	for id := range c.publishes {
		if _, ok := c.consumes[id]; !ok {
			count++
		}
	}

	return count
}

func (c *Counter) String() string {
	return fmt.Sprintf("tried publish: %v successful publish: %v, consume: %v, published not consumed: %v", c.triedPublishes, c.PublishCount(), c.ConsumeCount(), c.PublishedNotConsumed())
}
