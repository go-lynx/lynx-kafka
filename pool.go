package kafka

import (
	"sync"

	"github.com/go-lynx/lynx/log"
)

// GoroutinePool bounds concurrent tasks to its channel capacity. When full,
// Submit runs the task inline rather than blocking or dropping it.
type GoroutinePool struct {
	wg     sync.WaitGroup
	ch     chan struct{}
	closed bool
	mu     sync.RWMutex
}

// NewGoroutinePool creates a pool allowing up to size concurrent tasks.
func NewGoroutinePool(size int) *GoroutinePool {
	return &GoroutinePool{
		ch:     make(chan struct{}, size),
		closed: false,
	}
}

// Submit runs task on a pooled goroutine, or inline if the pool is at capacity.
// Tasks submitted after Close are dropped. Panics are recovered and logged.
func (p *GoroutinePool) Submit(task func()) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.wg.Add(1)
	p.mu.Unlock()

	select {
	case p.ch <- struct{}{}: // Acquire token
		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.Errorf("GoroutinePool task panic recovered: %v", r)
				}
				<-p.ch // Release token
				p.wg.Done()
			}()
			if task != nil {
				task()
			}
		}()
	default:
		// Pool saturated: run inline so the caller applies backpressure.
		defer p.wg.Done()
		if task == nil {
			return
		}
		defer func() {
			if r := recover(); r != nil {
				log.Errorf("GoroutinePool task panic recovered: %v", r)
			}
		}()
		task()
	}
}

func (p *GoroutinePool) Wait() {
	p.wg.Wait()
}

// Close stops accepting new tasks and blocks until in-flight tasks finish.
func (p *GoroutinePool) Close() {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	p.wg.Wait()
}

func (p *GoroutinePool) IsClosed() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.closed
}

// PoolConfig goroutine pool configuration
type PoolConfig struct {
	Size int
}

// DefaultPoolConfig returns a pool sized at 30 concurrent tasks.
func DefaultPoolConfig() *PoolConfig {
	return &PoolConfig{
		Size: 30,
	}
}
