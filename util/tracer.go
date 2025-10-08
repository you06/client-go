package util

import "context"

type TracerKeyType struct{}

var TracerKey = TracerKeyType{}

func WithTracer(ctx context.Context, tag string) context.Context {
	return context.WithValue(ctx, TracerKey, tag)
}

func GetTracer(ctx context.Context) (string, bool) {
	v := ctx.Value(TracerKey)
	if v == nil {
		return "", false
	}
	startTS, ok := v.(string)
	return startTS, ok
}
