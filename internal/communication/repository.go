package communication

import (
	"context"
	"math"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type Repository struct {
	Conn driver.Conn
}

type commSearchParams struct {
	// Starting timestamp given in nanoseconds since Unix epoch.
	// All matched communication must start at or after this timestamp.
	FromUnixNano *uint64

	// End timestamp given in nanoseconds since Unix epoch.
	// All matched communication must start before this timestamp.
	ToUnixNano *uint64

	// Trace ID to which the spans representing the called entity must belong.
	TraceID string

	// Git commit identifier with which the spans representing the called entity must be associated.
	// If left empty, the search is explicitly restricted to spans that have no associated commit.
	CommitHash string
}

// findCommunication searches the database for parent-child span pairs. The child and parent span pairs are grouped by the visualization objects they represent,
// where the parent span indicates the source object and the child indicates the target object. A single communication is inferred for any such source-target pair.
// For each communication, general metrics are computed.
func (r *Repository) findCommunication(ctx context.Context, landscapeToken string, searchParams commSearchParams) (CommSummary, error) {

	queryParams := make([]any, 0, 5)
	queryParams = append(queryParams, clickhouse.Named("landscapeToken", landscapeToken))
	var conditions strings.Builder

	if searchParams.FromUnixNano != nil {
		conditions.WriteString(" AND c.Timestamp_ns >= @from")
		queryParams = append(queryParams, clickhouse.Named("from", *searchParams.FromUnixNano))
	}

	if searchParams.ToUnixNano != nil {
		conditions.WriteString(" AND c.Timestamp_ns < @to")
		queryParams = append(queryParams, clickhouse.Named("to", *searchParams.ToUnixNano))
	}

	if searchParams.TraceID != "" {
		conditions.WriteString(" AND c.TraceId = @traceId")
		queryParams = append(queryParams, clickhouse.Named("traceId", searchParams.TraceID))
	}

	if searchParams.CommitHash != "" {
		conditions.WriteString(" AND c.SpanAttributes['vcs.ref.head.revision'] = @commit")
		queryParams = append(queryParams, clickhouse.Named("commit", searchParams.CommitHash))
	}

	cs := []Comm{}

	err := r.Conn.Select(ctx, &cs, `
		// Find communications and aggregate metrics
		WITH comms AS (
			SELECT
				p.ExplorvizTelemetryKey AS SourceTelemetryKey,
				c.ExplorvizTelemetryKey AS TargetTelemetryKey,
				toFloat64(COUNT(c.SpanId)) AS RequestCount,
				toFloat64(COUNT(DISTINCT c.ExplorvizFuncName)) AS FunctionCount,
				toFloat64(sum(c.Duration)) AS ExecutionTime,
				min(c.Timestamp_ns) AS FromUnixNano,
				max(c.Timestamp_ns + c.Duration) AS ToUnixNano
			FROM otel_traces c
			INNER JOIN otel_traces p
				ON c.ParentSpanId = p.SpanId
				AND c.ExplorvizTokenId = p.ExplorvizTokenId
			WHERE
				c.ExplorvizTokenId = @landscapeToken
				`+conditions.String()+`
			GROUP BY
				c.ExplorvizTelemetryKey, p.ExplorvizTelemetryKey
		)

		// Combine backwards and forwards calls into bidirectional communication
		SELECT
			a.SourceTelemetryKey AS SourceTelemetryKey,
			a.TargetTelemetryKey AS TargetTelemetryKey,
			b.SourceTelemetryKey != '' AS Bidirectional,
			least(a.FromUnixNano, firstNonDefault(b.FromUnixNano, a.FromUnixNano)) AS FromUnixNano,
			greatest(a.ToUnixNano, firstNonDefault(b.ToUnixNano, a.ToUnixNano)) AS ToUnixNano,
			map (
				'requestCount', a.RequestCount + b.RequestCount,
				'functionCount', a.FunctionCount + b.FunctionCount,
				'executionTime', a.ExecutionTime + b.ExecutionTime
			) AS Metrics
		FROM comms a
		LEFT JOIN comms b
			ON a.SourceTelemetryKey = b.TargetTelemetryKey
			AND a.TargetTelemetryKey = b.SourceTelemetryKey
		WHERE
			a.SourceTelemetryKey <= a.TargetTelemetryKey
			OR b.SourceTelemetryKey = '';
	`, queryParams...)
	if err != nil {
		return CommSummary{}, err
	}

	if len(cs) == 0 {
		return CommSummary{
			Comms:          []Comm{},
			FromUnixNano:   0,
			ToUnixNano:     0,
			MetricsSummary: map[string]MetricRange{},
		}, nil
	}

	var from int64 = math.MaxInt64
	var to int64 = math.MinInt64
	ms := make(map[string]MetricRange)
	for i, c := range cs {
		cs[i].ID = c.SourceTelemetryKey + "-" + c.TargetTelemetryKey
		cs[i].Name = c.SourceTelemetryKey + " - " + c.TargetTelemetryKey
		from = min(cs[i].FromUnixNano, from)
		to = max(cs[i].ToUnixNano, to)
		for k, v := range c.Metrics {
			if m, ok := ms[k]; ok {
				m.Min = min(m.Min, v)
				m.Max = max(m.Max, v)
				ms[k] = m
			} else {
				ms[k] = MetricRange{
					Min: v,
					Max: v,
				}
			}
		}
	}

	return CommSummary{
		Comms:          cs,
		FromUnixNano:   from,
		ToUnixNano:     to,
		MetricsSummary: ms,
	}, nil
}
