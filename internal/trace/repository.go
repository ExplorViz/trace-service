package trace

import (
	"context"
	"log/slog"
	"strconv"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type Repository struct {
	Conn driver.Conn
}

type spanSearchParams struct {
	// Text tokens to search within spans. By default, only the span name is searched
	SearchString *string

	// Reports whether the search string should also be applied to span and resource attribute keys
	IncludeAttribKeys bool

	// Reports whether the search string should also be applied to span and resource attribute values
	IncludeAttribVals bool

	TelemetryKey *string
	ServiceName  *string

	Kind *string

	FromUnixNano *uint64
	ToUnixNano   *uint64

	TraceID    *string
	CommitHash *string

	SortBy spanSearchSorting

	// Limits the number of retrieved rows, for use with paginiation.
	Limit *uint64

	// Specifies the last received span from the previous request.
	Cursor *spanSearchCursor
}

type spanSearchSorting int

const (
	SortNewest spanSearchSorting = iota
	SortOldest
	SortDuration
)

// A spanSearchCursor specifies the last seen span from a prior request.
// This can be used for pagination.
type spanSearchCursor struct {
	SpanID    string
	Timestamp uint64
	Duration  uint64
}

// findLandscapeSpans searches the database for spans associated with the given landscape.
// The search space can be restricted using a variety of filter options (see [spanSearchParams]).
func (r *Repository) findLandscapeSpans(ctx context.Context, landscapeToken string, params spanSearchParams) ([]Span, error) {
	queryParams := make([]any, 0, 13)
	var conditions strings.Builder

	queryParams = append(queryParams, clickhouse.Named("landscapeToken", landscapeToken))

	if params.SearchString != nil {
		conditions.WriteString(" AND (hasAllTokens(Name, @name)")
		queryParams = append(queryParams, clickhouse.Named("name", *params.SearchString))

		if params.IncludeAttribKeys {
			conditions.WriteString(`
				OR (
					hasAllTokens(mapKeys(SpanAttributes), @name)
					OR hasAllTokens(mapKeys(ResourceAttributes), @name)
				)`)
		}

		if params.IncludeAttribVals {
			conditions.WriteString(`
				OR (
					hasAllTokens(mapValues(SpanAttributes), @name)
					OR hasAllTokens(mapValues(ResourceAttributes), @name)
				)`)
		}
		conditions.WriteString(")")
	}

	if params.ServiceName != nil {
		conditions.WriteString(" AND ServiceName = @serviceName")
		queryParams = append(queryParams, clickhouse.Named("serviceName", *params.ServiceName))
	}

	if params.TelemetryKey != nil {
		conditions.WriteString(" AND ExplorvizTelemetryKey = @telemetryKey")
		queryParams = append(queryParams, clickhouse.Named("telemetryKey", *params.TelemetryKey))
	}

	if params.Kind != nil {
		conditions.WriteString(" AND SpanKind = @kind")
		queryParams = append(queryParams, clickhouse.Named("kind", *params.Kind))
	}

	if params.FromUnixNano != nil {
		conditions.WriteString(" AND Timestamp_ns >= @from")
		queryParams = append(queryParams, clickhouse.Named("from", *params.FromUnixNano))
	}

	if params.ToUnixNano != nil {
		conditions.WriteString(" AND Timestamp_ns < @to")
		queryParams = append(queryParams, clickhouse.Named("to", *params.ToUnixNano))
	}

	if params.TraceID != nil {
		conditions.WriteString(" AND TraceId = @traceId")
		queryParams = append(queryParams, clickhouse.Named("traceId", *params.TraceID))
	}

	if params.CommitHash != nil {
		conditions.WriteString(" AND CommitHash = @commitHash")
		queryParams = append(queryParams, clickhouse.Named("commitHash", *params.CommitHash))
	}

	if params.Cursor != nil {
		switch params.SortBy {
		case SortNewest:
			conditions.WriteString(`
				AND (
					Timestamp_ns < @cursorTimestamp
					OR (Timestamp_ns = @cursorTimestamp AND SpanId > @cursorId)
				)`)
		case SortOldest:
			conditions.WriteString(`
				AND (
					Timestamp_ns > @cursorTimestamp
					OR (Timestamp_ns = @cursorTimestamp AND SpanId > @cursorId)
				)`)

		case SortDuration:
			conditions.WriteString(`
				AND (
					Duration < @cursorDuraton
					OR (
						Duration = @cursorDuration
						AND (
							Timestamp_ns < @cursorTimestamp
							OR (Timestamp_ns = @cursorTimestamp AND SpanId > @cursorId)
						)
					)
				)`)
		default:
			slog.Error("Received invalid sorting order", "SortBy", params.SortBy)
		}

		queryParams = append(queryParams,
			clickhouse.Named("cursorId", params.Cursor.SpanID),
			clickhouse.Named("cursorTimestamp", params.Cursor.Timestamp),
			clickhouse.Named("cursorDuration", params.Cursor.Duration),
		)
	}

	ordering := ""
	switch params.SortBy {
	case SortNewest:
		ordering += " ORDER BY Timestamp_ns DESC, SpanId ASC"
	case SortOldest:
		ordering += " ORDER BY Timestamp_ns ASC, SpanId ASC"
	case SortDuration:
		ordering += " ORDER BY Duration DESC, Timestamp_ns DESC, SpanId ASC"
	default:
		slog.Error("Received invalid sorting order", "SortBy", params.SortBy)
	}

	queryLimit := ""
	if params.Limit != nil {
		queryLimit = " LIMIT @limit"
		queryParams = append(queryParams, clickhouse.Named("limit", *params.Limit))
	}

	spans := []Span{}

	err := r.Conn.Select(ctx, &spans, `
		SELECT
			SpanId AS SpanID,
			TraceId AS TraceID,
			ParentSpanId AS ParentSpanID,
			SpanName AS Name,
			SpanKind AS Kind,
			ExplorvizTelemetryKey AS TelemetryKey,
			ServiceName,
			ScopeName AS InstrumentationScope,
			Timestamp_ns AS StartUnixNano,
			Timestamp_ns + Duration AS EndUnixNano,
			SpanAttributes AS SpanAttribs,
			ResourceAttributes AS ResourceAttribs
		FROM otel_traces
		WHERE
			ExplorvizTokenId = @landscapeToken
			`+conditions.String()+ordering+queryLimit, queryParams...)
	if err != nil {
		return []Span{}, err
	}

	return spans, nil
}

// findCommunicationSpans searches the database for any spans starting within the time span given by fromUnixNano (inclusive) and toUnixNano (exclusive)
// where the span has a parent span such that the span pair's visualization object IDs match any one of the provided [commSpansRequest]s. To restrict search
// for spans to those associated with a specific commit, the commitHash value can be used. If left empty, then the search is explicitly restricted to spans
// that have no associated commit. A limit and an offset can optionally be specified for pagination.
func (r *Repository) findCommunicationSpans(
	ctx context.Context, landscapeToken string, sreqs []commSpansRequest, fromUnixNano uint64, toUnixNano uint64, commitHash string, limit uint64, offset uint64,
) (CommSpans, error) {

	comms := make([]clickhouse.GroupSet, len(sreqs))
	for i, sreq := range sreqs {
		comms[i] = clickhouse.GroupSet{Value: []any{
			min(sreq.SourceTelemetryKey, sreq.TargetTelemetryKey),
			max(sreq.SourceTelemetryKey, sreq.TargetTelemetryKey),
		}}
	}

	params := []any{
		clickhouse.Named("landscapeToken", landscapeToken),
		clickhouse.Named("comms", comms),
		clickhouse.Named("from", fromUnixNano),
		clickhouse.Named("to", toUnixNano),
		clickhouse.Named("commit", commitHash),
	}

	queryLimit := ""
	if limit > 0 {
		queryLimit = "LIMIT " + strconv.FormatUint(limit, 10)
	}
	if offset > 0 {
		queryLimit += " OFFSET " + strconv.FormatUint(offset, 10)
	}

	spans := []Span{}

	err := r.Conn.Select(ctx, &spans, `
		WITH comms AS (
			SELECT
				c.TraceId AS TraceId,

				c.SpanId AS ChildSpanId,
				c.SpanName AS ChildSpanName,
				c.SpanKind AS ChildSpanKind,
				c.Timestamp_ns AS ChildStartTime,
				c.Timestamp_ns + c.Duration AS ChildEndTime,
				c.SpanAttributes AS ChildSpanAttributes,
				c.ResourceAttributes AS ChildResourceAttributes,

				p.SpanId AS ParentSpanId,
				p.ParentSpanId AS ParentParentSpanId,
				p.SpanName AS ParentSpanName,
				p.SpanKind AS ParentSpanKind,
				p.Timestamp_ns AS ParentStartTime,
				p.Timestamp_ns + p.Duration AS ParentEndTime,
				p.SpanAttributes AS ParentSpanAttributes,
				p.ResourceAttributes AS ParentResourceAttributes
			FROM otel_traces c
			INNER JOIN otel_traces p
				ON c.ParentSpanId = p.SpanId
				AND c.ExplorvizTokenId = p.ExplorvizTokenId
			WHERE
				c.ExplorvizTokenId = @landscapeToken
				AND (
					least(c.ExplorvizTelemetryKey, p.ExplorvizTelemetryKey),
					greatest(c.ExplorvizTelemetryKey, p.ExplorvizTelemetryKey)
				) IN (@comms)
				AND c.Timestamp_ns >= @from
				AND c.Timestamp_ns <= @to
				AND coalesce(c.SpanAttributes['vcs.ref.head.revision'], '') = @commit
		)

		SELECT
			TraceId AS TraceID,
			ChildSpanId AS SpanID,
			ParentSpanId AS ParentSpanID,
			ChildSpanName AS Name,
			ChildSpanKind AS Kind,
			ChildStartTime AS StartUnixNano,
			ChildEndTime AS EndUnixNano,
			ChildSpanAttributes AS SpanAttribs,
			ChildResourceAttributes AS ResourceAttribs
		FROM comms

		UNION DISTINCT

		SELECT
			TraceId AS TraceID,
			ParentSpanId AS SpanID,
			ParentParentSpanId AS ParentSpanID,
			ParentSpanName AS Name,
			ParentSpanKind AS Kind,
			ParentStartTime AS StartUnixNano,
			ParentEndTime AS EndUnixNano,
			ParentSpanAttributes AS SpanAttribs,
			ParentResourceAttributes AS ResourceAttribs
		FROM comms

		ORDER BY StartUnixNano ASC
		`+queryLimit, params...)
	if err != nil {
		return CommSpans{}, err
	}

	cs := CommSpans{
		Spans: make(map[string]Span, len(spans)),
		Pairs: make([]SpanPair, 0, len(spans)/2),
	}

	for _, span := range spans {
		cs.Spans[span.SpanID] = span
	}

	for _, span := range spans {
		if _, ok := cs.Spans[span.ParentSpanID]; ok {
			cs.Pairs = append(cs.Pairs, SpanPair{
				ParentSpanID: span.ParentSpanID,
				ChildSpanID:  span.SpanID,
			})
		}
	}

	return cs, nil
}

func (r Repository) deleteAll(ctx context.Context, landscapeToken string) error {
	return r.Conn.Exec(ctx, "DELETE FROM otel_traces WHERE ExplorvizTokenId = ?;", landscapeToken)
}
