package grpc

import (
	"context"
	"errors"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/ratelimit"
	"golang.org/x/time/rate"
)

var ErrTooManyRequests = errors.New("number of permitted requests exceeded")

var rateLimit *rateLimiter

func RateLimit() *rateLimiter {
	if rateLimit == nil {
		OneThousandReqPS()
	}
	return rateLimit
}

type rateLimiter struct {
	l *rate.Limiter
}

func (r *rateLimiter) Limiter() *rate.Limiter {
	return r.l
}

func (r *rateLimiter) Limit(_ context.Context) error {
	if r.l.Allow() {
		return nil
	}
	return ErrTooManyRequests
}

func NReqPS(n int) ratelimit.Limiter {
	return NewRateLimiter(time.Millisecond, n)
}
func OneThousandReqPS() ratelimit.Limiter {
	return NReqPS(1000)
}
func TwoThousandReqPS() ratelimit.Limiter {
	return NReqPS(2000)
}

func NewRateLimiter(s time.Duration, t int) ratelimit.Limiter {
	rateLimit = &rateLimiter{
		l: rate.NewLimiter(rate.Every(s), t),
	}
	return rateLimit
}
