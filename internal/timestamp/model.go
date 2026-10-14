package timestamp

// A Timestamp represents a time interval in which communication between visualization entities occurs.
type Timestamp struct {
	// Starting point of a time range within which some number of spans started,
	// as Unix Epoch nanosecond timestamp (inclusive).
	FromUnixNano int64 `json:"fromUnixNano,string"`

	// End point of a time range within which some number of spans started,
	// as Unix Epoch nanosecond timestamp (exclusive).
	ToUnixNano int64 `json:"toUnixNano,string"`

	// The number of spans to start within the time interval represented by the Timestamp.
	SpanCount uint64 `json:"spanCount"`

	// The IDs of all distinct traces that have a representing span that starts within this time interval.
	TraceIDs []string `json:"traceIds"`
}
