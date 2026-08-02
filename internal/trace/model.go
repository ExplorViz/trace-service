package trace

type Span struct {
	SpanID       string   `json:"spanId"`
	ChildSpanIDs []string `json:"childSpanIds"`
	Name         string   `json:"name"`
	Kind         string   `json:"kind"`

	StartTime int64 `json:"startTime,string"`
	EndTime   int64 `json:"endTime,string"`

	ResourceAttribs map[string]string `json:"resourceAttributes"`
	SpanAttribs     map[string]string `json:"spanAttributes"`
}

type spanRequest struct {
	SourceVizObjId string `json:"source"`
	TargetVizObjId string `json:"target"`
}
