package attrib

import (
	"encoding/hex"

	"go.opentelemetry.io/otel/attribute"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// A SpanReader groups a Protobuf [tracepb.Span] together with its [commonpb.InstrumentationScope]
// and [resourcepb.Resource]. It provides helper methods for efficient lookup of attributes
// by leveraging pre-constructed maps.
//
// Should not be initialized directly; use [NewSpanReader] instead.
type SpanReader struct {
	Span     *tracepb.Span
	Scope    *commonpb.InstrumentationScope
	Resource *resourcepb.Resource

	spanAttributes     map[string]*commonpb.AnyValue
	scopeAttributes    map[string]*commonpb.AnyValue
	resourceAttributes map[string]*commonpb.AnyValue
}

// NewSpanReader initializes a new SpanReader for the given span, scope, and resource.
// It should be ensured that the span, scope, and resource belong to one another.
//
// Panics if any parameter is nil.
func NewSpanReader(s *tracepb.Span, sc *commonpb.InstrumentationScope, rs *resourcepb.Resource) SpanReader {
	if s == nil || sc == nil || rs == nil {
		panic("SpanReader constructed with nil value")
	}

	return SpanReader{
		Span:     s,
		Scope:    sc,
		Resource: rs,

		spanAttributes:     attrsToMap(s.GetAttributes()),
		scopeAttributes:    attrsToMap(sc.GetAttributes()),
		resourceAttributes: attrsToMap(rs.GetAttributes()),
	}
}

// SpanAttribute is a helper for efficient access of the underlying span's attributes.
// Returns nil if no attribute with the specified key exists.
func (sr SpanReader) SpanAttribute(key attribute.Key) *commonpb.AnyValue {
	return sr.spanAttributes[string(key)]
}

// ScopeAttribute is a helper for efficient access of the underlying instrumentation scope's attributes.
// Returns nil if no attribute with the specified key exists.
func (sr SpanReader) ScopeAttribute(key attribute.Key) *commonpb.AnyValue {
	return sr.scopeAttributes[string(key)]
}

// ResourceAttribute is a helper for efficient access of the underlying resource's attributes.
// Returns nil if no attribute with the specified key exists.
func (sr SpanReader) ResourceAttribute(key attribute.Key) *commonpb.AnyValue {
	return sr.resourceAttributes[string(key)]
}

// TraceID returns the hex string representation of the span's trace ID
// Returns the empty string if the span is missing a trace ID.
func (sr SpanReader) TraceID() string {
	return hex.EncodeToString(sr.Span.GetTraceId())
}

// SpanID returns the hex string representation of the span's span ID.
// Returns the empty string if the span is missing a span ID.
func (sr SpanReader) SpanID() string {
	return hex.EncodeToString(sr.Span.GetSpanId())
}

// ParentSpanID returns the hex string representation of the span's parent span ID.
// Returns the empty string if the span has no parent.
func (sr SpanReader) ParentSpanID() string {
	return hex.EncodeToString(sr.Span.GetParentSpanId())
}

// TokenID looks for an attribute specifying the ID of an ExplorViz landscape token.
// The searched attribute key is given by [ExplorVizAttributes.LandscapeTokenID.Key].
// The resource attributes are considered first. If no token ID can be extracted from the resource,
// then we look at the span attributes as a fallback. If this also fails, the empty string is returned.
func (sr SpanReader) TokenID() string {
	if v := sr.ResourceAttribute(ExplorVizAttributes.LandscapeTokenID.Key).GetStringValue(); v != "" {
		return v
	}
	return sr.SpanAttribute(ExplorVizAttributes.LandscapeTokenID.Key).GetStringValue()
}

// TokenID looks for an attribute specifying the secret of an ExplorViz landscape token.
// The searched attribute key is given by [ExplorVizAttributes.LandscapeTokenSecret.Key].
// The resource attributes are considered first. If no token ID can be extracted from the resource,
// then we look at the span attributes as a fallback. If this also fails, the empty string is returned.
func (sr SpanReader) TokenSecret() string {
	if v := sr.ResourceAttribute(ExplorVizAttributes.LandscapeTokenSecret.Key).GetStringValue(); v != "" {
		return v
	}
	return sr.SpanAttribute(ExplorVizAttributes.LandscapeTokenSecret.Key).GetStringValue()
}

func attrsToMap(attrs []*commonpb.KeyValue) map[string]*commonpb.AnyValue {
	m := make(map[string]*commonpb.AnyValue, len(attrs))
	for _, kv := range attrs {
		m[kv.GetKey()] = kv.GetValue()
	}
	return m
}
