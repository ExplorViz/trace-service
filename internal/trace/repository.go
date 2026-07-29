package trace

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type Repository struct {
	Conn driver.Conn
}

func (r Repository) deleteAll(ctx context.Context, landscapeToken string) error {
	return r.Conn.Exec(ctx, "DELETE FROM otel_traces WHERE ExplorvizTokenId = ?;", landscapeToken)
}
