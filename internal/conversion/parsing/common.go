package parsing

import (
	"go.opentelemetry.io/otel/attribute"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

type SpanReader struct {
	Span     *tracepb.Span
	Scope    *commonpb.InstrumentationScope
	Resource *resourcepb.Resource

	spanAttributes     map[string]*commonpb.AnyValue
	scopeAttributes    map[string]*commonpb.AnyValue
	resourceAttributes map[string]*commonpb.AnyValue
}

func (s SpanReader) SpanAttribute(key attribute.Key) *commonpb.AnyValue {
	return s.spanAttributes[string(key)]
}

func (s SpanReader) ResourceAttribute(key attribute.Key) *commonpb.AnyValue {
	return s.resourceAttributes[string(key)]
}

func NewSpanReader(s *tracepb.Span, sc *commonpb.InstrumentationScope, rs *resourcepb.Resource) SpanReader {
	return SpanReader{
		spanAttributes:     attrsToMap(s.Attributes),
		scopeAttributes:    attrsToMap(sc.Attributes),
		resourceAttributes: attrsToMap(rs.Attributes),
	}
}

type SpanParser func(s SpanReader) (SpanEntity, error)

type SpanEntity interface {
	Id() string
}

func attrsToMap(attrs []*commonpb.KeyValue) map[string]*commonpb.AnyValue {
	m := make(map[string]*commonpb.AnyValue, len(attrs))
	for _, kv := range attrs {
		m[kv.GetKey()] = kv.GetValue()
	}
	return m
}
