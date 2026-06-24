package attrib

import (
	"encoding/hex"
	"testing"

	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

const defaultSpanID = "5fb397be34d26b51"
const defaultTraceID = "5b8aa5a2d2c872e8321cf37308d69df2"
const defaultParentID = "051581bf3cb55c13"
const defaultSpanName = "hello-greetings"

const defaultTokenID = "mytokenvalue"
const defaultTokenSecret = "mytokensecret"

const defaultServiceName = "trace-service"
const defaultScopeName = "my-scope"
const defaultFunctionFQN = "net.explorviz.example.MyClass.myMethod"

func strAnyVal(s string) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: s}}
}

func defaultTestSpan() *tracepb.Span {
	bSpanID, _ := hex.DecodeString(defaultSpanID)
	bTraceID, _ := hex.DecodeString(defaultTraceID)
	bParentID, _ := hex.DecodeString(defaultParentID)

	attribs := []*commonpb.KeyValue{
		{Key: string(semconv.CodeFunctionNameKey), Value: strAnyVal(defaultFunctionFQN)},
	}

	return &tracepb.Span{
		SpanId:       bSpanID,
		TraceId:      bTraceID,
		ParentSpanId: bParentID,

		Name:              defaultSpanName,
		StartTimeUnixNano: 0,
		EndTimeUnixNano:   10,

		Attributes: attribs,
	}
}

func defaultTestResource() *resourcepb.Resource {
	return &resourcepb.Resource{
		Attributes: []*commonpb.KeyValue{
			{Key: string(ExplorVizAttributes.LandscapeTokenID.Key), Value: strAnyVal(defaultTokenID)},
			{Key: string(ExplorVizAttributes.LandscapeTokenSecret.Key), Value: strAnyVal(defaultTokenSecret)},
			{Key: string(semconv.ServiceNameKey), Value: strAnyVal(defaultServiceName)},
		},
	}
}

func defaultTestScope() *commonpb.InstrumentationScope {
	return &commonpb.InstrumentationScope{
		Name: defaultScopeName,
		Attributes: []*commonpb.KeyValue{
			{Key: string(ExplorVizAttributes.LandscapeTokenID.Key), Value: strAnyVal(defaultTokenID)},
			{Key: string(ExplorVizAttributes.LandscapeTokenSecret.Key), Value: strAnyVal(defaultTokenSecret)},
			{Key: string(semconv.ServiceNameKey), Value: strAnyVal(defaultServiceName)},
		},
	}
}

func TestSpanID(t *testing.T) {
	sr := NewSpanReader(defaultTestSpan(), &commonpb.InstrumentationScope{}, &resourcepb.Resource{})
	spanID := sr.SpanID()

	if spanID != defaultSpanID {
		t.Errorf("sr.SpanID() = %s, want %s", spanID, defaultSpanID)
	}
}

func TestSpanIDEmpty(t *testing.T) {
	sr := NewSpanReader(&tracepb.Span{}, &commonpb.InstrumentationScope{}, &resourcepb.Resource{})
	spanID := sr.SpanID()

	if spanID != "" {
		t.Errorf("sr.SpanID() should return empty string if SpanId unset, got %s", spanID)
	}
}

func TestTraceID(t *testing.T) {
	sr := NewSpanReader(defaultTestSpan(), &commonpb.InstrumentationScope{}, &resourcepb.Resource{})
	traceID := sr.TraceID()

	if traceID != defaultTraceID {
		t.Errorf("sr.TraceID() = %s, want %s", traceID, defaultTraceID)
	}
}

func TestTraceIDEmpty(t *testing.T) {
	sr := NewSpanReader(&tracepb.Span{}, &commonpb.InstrumentationScope{}, &resourcepb.Resource{})
	traceID := sr.TraceID()

	if traceID != "" {
		t.Errorf("sr.TraceID() should return empty string if TraceId unset, got %s", traceID)
	}
}

func TestParentID(t *testing.T) {
	sr := NewSpanReader(defaultTestSpan(), &commonpb.InstrumentationScope{}, &resourcepb.Resource{})
	parentID := sr.ParentSpanID()

	if parentID != defaultParentID {
		t.Errorf("sr.ParentSpanID() = %s, want %s", parentID, defaultParentID)
	}
}

func TestParentIDEmpty(t *testing.T) {
	sr := NewSpanReader(&tracepb.Span{}, &commonpb.InstrumentationScope{}, &resourcepb.Resource{})
	parentID := sr.ParentSpanID()

	if parentID != "" {
		t.Errorf("sr.TraceID() should return empty string if span has no parent, got %s", parentID)
	}
}

func TestTokenID(t *testing.T) {
	sr := NewSpanReader(&tracepb.Span{}, &commonpb.InstrumentationScope{}, defaultTestResource())
	id := sr.TokenID()

	if id == "" {
		t.Errorf(`failed to determine token ID from resource, sr.TokenID() == ""`)
	}

	if id != defaultTokenID {
		t.Errorf("read unexpected token ID from resource, sr.TokenID() = %s, want %s", id, defaultTokenID)
	}
}

func TestTokenIDInSpan(t *testing.T) {
	sr := NewSpanReader(
		&tracepb.Span{Attributes: []*commonpb.KeyValue{
			{Key: string(ExplorVizAttributes.LandscapeTokenID.Key), Value: strAnyVal(defaultTokenID)},
		}},
		&commonpb.InstrumentationScope{}, &resourcepb.Resource{})

	id := sr.TokenID()

	if id == "" {
		t.Errorf(`failed to determine token ID from span, sr.TokenID() == ""`)
	}

	if id != defaultTokenID {
		t.Errorf("read unexpected token ID from span, sr.TokenID() = %s, want %s", id, defaultTokenID)
	}
}

func TestTokenIDMissing(t *testing.T) {
	sr := NewSpanReader(&tracepb.Span{}, &commonpb.InstrumentationScope{}, &resourcepb.Resource{})

	id := sr.TokenID()

	if id != "" {
		t.Errorf("sr.TokenID() should return emtpy string if no token attribute provided, got %s", id)
	}
}

func TestTokenSecret(t *testing.T) {
	sr := NewSpanReader(&tracepb.Span{}, &commonpb.InstrumentationScope{}, defaultTestResource())
	sec := sr.TokenSecret()

	if sec == "" {
		t.Errorf(`failed to determine token secret from resource, sr.TokenSecret() == ""`)
	}

	if sec != defaultTokenSecret {
		t.Errorf("read unexpected token secret fom resource, sr.TokenSecret() = %s, want %s", sec, defaultTokenSecret)
	}
}

func TestTokenSecretInSpan(t *testing.T) {
	sr := NewSpanReader(
		&tracepb.Span{Attributes: []*commonpb.KeyValue{
			{Key: string(ExplorVizAttributes.LandscapeTokenSecret.Key), Value: strAnyVal(defaultTokenSecret)},
		}},
		&commonpb.InstrumentationScope{}, &resourcepb.Resource{})
	sec := sr.TokenSecret()

	if sec == "" {
		t.Errorf(`failed to determine token secret from span, sr.TokenSecret() == ""`)
	}

	if sec != defaultTokenSecret {
		t.Errorf("read unexpected token secret from span, sr.TokenSecret() = %s, want %s", sec, defaultTokenSecret)
	}
}

func TestTokenSecretIDMissing(t *testing.T) {
	sr := NewSpanReader(&tracepb.Span{}, &commonpb.InstrumentationScope{}, &resourcepb.Resource{})

	sec := sr.TokenSecret()

	if sec != "" {
		t.Errorf("sr.TokenSecret() should return emtpy string if no token attribute provided, got %s", sec)
	}
}

func TestParentSpanID(t *testing.T) {
	sr := NewSpanReader(defaultTestSpan(), &commonpb.InstrumentationScope{}, &resourcepb.Resource{})

	pID := sr.ParentSpanID()

	if pID == "" {
		t.Errorf("sr.ParentSpanID() returned empty string, want %s", defaultParentID)
	}

	if pID != defaultParentID {
		t.Errorf("unexpected value for parent span ID, sr.ParentSpanID() = %s, want %s", pID, defaultParentID)
	}
}

func TestParentSpanIDEmpty(t *testing.T) {
	sr := NewSpanReader(&tracepb.Span{}, &commonpb.InstrumentationScope{}, &resourcepb.Resource{})

	pID := sr.ParentSpanID()

	if pID != "" {
		t.Errorf("expected empty string from sr.ParentSpanID(), got %s", pID)
	}
}

func TestNewSpanReader(t *testing.T) {
	sr := NewSpanReader(defaultTestSpan(), defaultTestScope(), defaultTestResource())

	if sr.Span == nil {
		t.Errorf("got nil value for Span after call to NewSpanReader()")
	}
	if sr.Scope == nil {
		t.Errorf("got nil value for Scope after call to NewSpanReader()")
	}
	if sr.Resource == nil {
		t.Errorf("got nil value for Resource after call to NewSpanReader()")
	}
	if sr.spanAttributes == nil {
		t.Errorf("got nil value for span attribute map after call to NewSpanReader()")
	}
	if sr.scopeAttributes == nil {
		t.Errorf("got nil value for scope attribute map after call to NewSpanReader()")
	}
	if sr.resourceAttributes == nil {
		t.Errorf("got nil value for resource attribute map after call to NewSpanReader()")
	}
}

func TestNewSpanReaderNil(t *testing.T) {
	assertPanics(t, func() {
		NewSpanReader(nil, &commonpb.InstrumentationScope{}, &resourcepb.Resource{})
	}, "SpanReader constructor should panic on nil span")

	assertPanics(t, func() {
		NewSpanReader(&tracepb.Span{}, nil, &resourcepb.Resource{})
	}, "SpanReader constructor should panic on nil instrumentation scope")

	assertPanics(t, func() {
		NewSpanReader(&tracepb.Span{}, &commonpb.InstrumentationScope{}, nil)
	}, "SpanReader constructor should panic on nil resource")
}

func assertPanics(t *testing.T, f func(), format string, args ...any) {
	defer func() {
		if recover() == nil {
			t.Errorf(format, args...)
		}
	}()
	f()
}
