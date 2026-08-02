package trace

import (
	"context"
	"strconv"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type Repository struct {
	Conn driver.Conn
}

func (r *Repository) findSpans(
	ctx context.Context, landscapeToken string, sreqs []spanRequest, fromUnixNano uint64, toUnixNano uint64, commitHash string, limit uint64, offset uint64,
) ([]Span, error) {

	comms := make([]clickhouse.GroupSet, len(sreqs))
	for i, sreq := range sreqs {
		comms[i] = clickhouse.GroupSet{Value: []any{
			min(sreq.SourceVizObjId, sreq.TargetVizObjId),
			max(sreq.SourceVizObjId, sreq.TargetVizObjId),
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

	s := []Span{}

	err := r.Conn.Select(ctx, &s, `
		SELECT
			c.SpanId AS SpanID,
			children.ChildSpanIDs,
			c.SpanName AS Name,
			c.SpanKind AS Kind,
			c.Timestamp_ns AS StartTime,
			c.Timestamp_ns + c.Duration AS EndTime,
			c.SpanAttributes AS SpanAttribs,
			c.ResourceAttributes AS ResourceAttribs
		FROM otel_traces c
		INNER JOIN otel_traces p
			ON c.ParentSpanId = p.SpanId
			AND c.ExplorvizTokenId = p.ExplorvizTokenId
		LEFT JOIN (
			SELECT ParentSpanId, groupArray(SpanId) AS ChildSpanIDs
			FROM otel_traces
			GROUP BY ParentSpanId
		) children
			ON children.ParentSpanId = c.SpanId
		WHERE
			c.ExplorvizTokenId = @landscapeToken
			AND (
				least(c.ExplorvizVizObjectId, p.ExplorvizVizObjectId),
				greatest(c.ExplorvizVizObjectId, p.ExplorvizVizObjectId)
			) IN (@comms)
			AND c.Timestamp_ns >= @from
			AND c.Timestamp_ns <= @to
			AND coalesce(c.SpanAttributes['vcs.ref.head.revision'], '') = @commit
		`+queryLimit, params...)
	if err != nil {
		return []Span{}, err
	}

	return s, nil
}

func (r Repository) deleteAll(ctx context.Context, landscapeToken string) error {
	return r.Conn.Exec(ctx, "DELETE FROM otel_traces WHERE ExplorvizTokenId = ?;", landscapeToken)
}
