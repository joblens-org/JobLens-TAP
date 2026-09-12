package service

import "context"

// Limiter 非阻塞并发信号量
type Limiter struct {
	sem chan struct{}
}

func NewLimiter(max int) *Limiter {
	if max < 1 {
		max = 1
	}
	return &Limiter{sem: make(chan struct{}, max)}
}

func (l *Limiter) Acquire() bool {
	select {
	case l.sem <- struct{}{}:
		return true
	default:
		return false
	}
}

func (l *Limiter) AcquireCtx(ctx context.Context) bool {
	select {
	case l.sem <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

func (l *Limiter) Release() {
	<-l.sem
}
