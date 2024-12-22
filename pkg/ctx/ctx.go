package ctx

import (
	"context"
	"github.com/google/uuid"
	"sync"
)

const CtxIDKey = "context-id"

type CtxIDGeneratorType func() string

var (
	ctxIDGenerator CtxIDGeneratorType
	mutex          sync.Mutex
)

func DefaultCtxIDGenerator() {
	WithCtxIDGenerator(func() string {
		return uuid.NewString()
	})
}

func IDGenerator() CtxIDGeneratorType {
	return ctxIDGenerator
}

func WithCtxIDGenerator(gen CtxIDGeneratorType) {
	mutex.Lock()
	defer mutex.Unlock()
	ctxIDGenerator = gen
}

func WithCtxID(ctx context.Context) context.Context {
	if IDGenerator() == nil {
		DefaultCtxIDGenerator()
	}
	return SetCtxID(ctx, IDGenerator()())
}

func GetCtxID(ctx context.Context) string {
	v, ok := ctx.Value(CtxIDKey).(string)
	if !ok {
		v = ""
	}
	return v
}
func SetCtxID(ctx context.Context, v string) context.Context {
	return context.WithValue(ctx, CtxIDKey, v)
}

func New() context.Context {
	return context.Background()
}
