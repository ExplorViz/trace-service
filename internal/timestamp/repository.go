package timestamp

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type Repository struct {
	Conn  driver.Conn
	Table string
}

func (r *Repository) findTimestamps(
	ctx context.Context, landscapeToken string, startUnixNano uint64, endUnixNano uint64, bucketSizeNano uint64, commitHash string,
) ([]Timestamp, error) {

	ts := []Timestamp{}

	err := r.Conn.Select(ctx, &ts, `
		WITH
			intDiv(Timestamp_ns, ?) * ? AS bucket
		SELECT
			bucket AS EpochNano,
			count() AS SpanCount
		FROM otel_traces
		WHERE
			ExplorvizTokenId = ?
			AND Timestamp_ns >= ?
			AND Timestamp_ns < ?
			AND coalesce(SpanAttributes['vcs.ref.head.revision'], '') = ?
		GROUP BY bucket
		ORDER BY bucket;
	`, bucketSizeNano, bucketSizeNano, landscapeToken, startUnixNano, endUnixNano, commitHash)
	if err != nil {
		return nil, err
	}

	return ts, nil
}
