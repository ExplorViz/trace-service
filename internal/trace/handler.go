package trace

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
)

type Handler struct {
	repo Repository
}

func NewHandler(r Repository) Handler {
	return Handler{
		repo: r,
	}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /v3/landscapes/{landscapeToken}/spans", h.getLandscapeSpans)
	mux.HandleFunc("POST /v3/landscapes/{landscapeToken}/communication/spans", h.getCommunicationSpans)
	mux.HandleFunc("DELETE /v3/landscapes/{landscapeToken}/trace-data", h.deleteTraceData)
}

func (h *Handler) getLandscapeSpans(w http.ResponseWriter, r *http.Request) {
	lt := r.PathValue("landscapeToken")
	if lt == "" {
		http.Error(w, "Missing or invalid landscape token in path parameter", http.StatusBadRequest)
		return
	}

	query := r.URL.Query()
	params := spanSearchParams{
		SearchString:      strOrNil(query.Get("searchString")),
		IncludeAttribKeys: query.Get("includeAttributeKeys") != "",
		IncludeAttribVals: query.Get("includeAttributeValues") != "",
		TelemetryKey:      strOrNil(query.Get("telemetryKey")),
		ServiceName:       strOrNil(query.Get("serviceName")),
		Kind:              strOrNil(query.Get("kind")),
		TraceID:           strOrNil(query.Get("traceId")),
		FromUnixNano:      parseUintOrNil(query.Get("from")),
		ToUnixNano:        parseUintOrNil(query.Get("to")),
		CommitHash:        strOrNil(query.Get("commit")),
		Limit:             parseUintOrNil(query.Get("limit")),
	}

	switch sortBy := query.Get("sortBy"); sortBy {
	case "", "newest":
		params.SortBy = SortNewest
	case "oldest":
		params.SortBy = SortOldest
	case "duration":
		params.SortBy = SortDuration
	default:
		http.Error(w, fmt.Sprintf(`Invalid value %s for parameter "sortBy"`, sortBy), http.StatusBadRequest)
		return
	}

	cursorID := query.Get("cursorId")
	cursorTs := query.Get("cursorTimestamp")

	if cursorID != "" && cursorTs != "" {
		var parsedTs uint64
		var err error
		if parsedTs, err = strconv.ParseUint(cursorTs, 10, 64); err != nil {
			http.Error(w, "Cursor timestamp is not valid Uint64", http.StatusBadRequest)
			return
		}

		params.Cursor = &spanSearchCursor{
			SpanID:    cursorID,
			Timestamp: parsedTs,
		}
	} else if cursorID != "" || cursorTs != "" {
		http.Error(w, "Provided some, but not all cursor values", http.StatusBadRequest)
		return
	}

	spans, err := h.repo.findLandscapeSpans(r.Context(), lt, params)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := json.NewEncoder(w).Encode(spans); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (h *Handler) getCommunicationSpans(w http.ResponseWriter, r *http.Request) {
	lt := r.PathValue("landscapeToken")
	if lt == "" {
		http.Error(w, "Missing or invalid landscape token in path parameter", http.StatusBadRequest)
		return
	}

	query := r.URL.Query()

	from, err := strconv.ParseUint(query.Get("from"), 10, 64)
	if err != nil {
		from = 0
	}

	to, err := strconv.ParseUint(query.Get("to"), 10, 64)
	if err != nil {
		to = math.MaxUint64
	}

	commit := query.Get("commit")

	limit, err := strconv.ParseUint(query.Get("limit"), 10, 64)
	if err != nil {
		limit = 0
	}

	offset, err := strconv.ParseUint(query.Get("offset"), 10, 64)
	if err != nil {
		offset = 0
	}

	var sreqs []commSpansRequest
	if err := json.NewDecoder(r.Body).Decode(&sreqs); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	for _, sreq := range sreqs {
		if sreq.SourceTelemetryKey == "" || sreq.TargetTelemetryKey == "" {
			http.Error(w, "A request object is missing source or target visualization object ID", http.StatusBadRequest)
			return
		}
	}

	var cs CommSpans
	if len(sreqs) == 0 {
		cs = CommSpans{
			Spans: map[string]Span{},
			Pairs: []SpanPair{},
		}
	} else {
		if cs, err = h.repo.findCommunicationSpans(r.Context(), lt, sreqs, from, to, commit, limit, offset); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	if err := json.NewEncoder(w).Encode(cs); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (h *Handler) deleteTraceData(w http.ResponseWriter, r *http.Request) {
	lt := r.PathValue("landscapeToken")
	if lt == "" {
		http.Error(w, "Missing or invalid landscape token in path parameter", http.StatusBadRequest)
		return
	}

	if err := h.repo.deleteAll(r.Context(), lt); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := json.NewEncoder(w).Encode(fmt.Sprintf("Trace data successfully deleted for landscape %s", lt)); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func strOrNil(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func parseUintOrNil(s string) *uint64 {
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return nil
	}
	return &v
}
