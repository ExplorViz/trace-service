package timestamp

// A Timestamp represents the beginning of a time interval in which communication between visualization entities occurs.
type Timestamp struct {
	// Starting point of a time range within which some positive number of spans started, as Unix Epoch nanosecond timestamp.
	EpochNano int64 `json:"epochNano"`

	// The number of spans to start within the time interval represented by the Timestamp.
	SpanCount uint64 `json:"spanCount"`
}
