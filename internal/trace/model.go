package trace

// A TraceOverview is a surface-level summary of an OpenTelemetry trace.
type TraceOverview struct {
	TraceID string `json:"traceId"`

	// Name of the root span of the trace.
	Name string `json:"name"`

	// Minimum of the start times of all spans in the trace,
	// given in nanoseconds since the Unix epoch.
	StartUnixNano int64 `json:"startUnixNano,string"`

	// Maximum of the end times of all spans in the trace,
	// given in nanoseconds since the Unix epoch.
	EndUnixNano int64 `json:"endUnixNano,string"`

	// Number of spans that are part of this trace.
	SpanCount uint64 `json:"spanCount"`

	// The names of all services involved in the trace.
	Services []string `json:"services"`

	// Name of the service from which the root span originates.
	EntrypointService string `json:"entrypointService"`
}

// A Span is a detailed representation of an OpenTelemetry span.
type Span struct {
	SpanID       string `json:"spanId"`
	TraceID      string `json:"traceId"`
	ParentSpanID string `json:"parentSpanId,omitempty"`
	Name         string `json:"name"`
	Kind         string `json:"kind"`

	TelemetryKey         string `json:"telemetryKey,omitempty"`
	ServiceName          string `json:"serviceName"`
	InstrumentationScope string `json:"instrumentationScope"`

	StartUnixNano int64 `json:"startUnixNano,string"`
	EndUnixNano   int64 `json:"endUnixNano,string"`

	SpanAttribs     map[string]string `json:"spanAttributes"`
	ResourceAttribs map[string]string `json:"resourceAttributes"`
}

// A SpanPair contains the span IDs of two spans that are in a parent-child relationship.
type SpanPair struct {
	ParentSpanID string `json:"parentSpanId"`
	ChildSpanID  string `json:"childSpanId"`
}

// A CommSpans is used for exchanging detailed span information
// on the spans from which a particular communication was derived.
type CommSpans struct {
	// Maps the ID of a span to the detailed span information.
	Spans map[string]Span `json:"spans"`

	// Contains each parent-child span pair from which the communication was derived.
	Pairs []SpanPair `json:"pairs"`
}

// A commSpansRequest represents a pair of visualization objects for which
// detailed information of spans involved in their communication should be retrieved.
type commSpansRequest struct {
	SourceTelemetryKey string `json:"source"`
	TargetTelemetryKey string `json:"target"`
}
