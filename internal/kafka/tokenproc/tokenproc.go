// Package tokenproc is concerned with processing landscape token events,
// such as the creation of new tokens and the deletion of existing tokens.
package tokenproc

import (
	"context"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"github.com/ExplorViz/trace-service/internal/genproto/tokenpb"
	"github.com/ExplorViz/trace-service/internal/token"
)

// Run continuously fetches records from the given client, attempts to deserialize them
// as [tokenpb.TokenEvent]s and updates the provided [InMemTokenStore] accordingly.
func Run(ctx context.Context, cl *kgo.Client, ts *token.InMemTokenStore) {
	for {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			slog.Debug("exiting kafka token poll loop")
			break
		}
		fs.EachRecord(func(r *kgo.Record) {
			if r.Value == nil {
				ts.Delete(string(r.Key))
				slog.Debug("received tombstone token record, deleting token", "tokenID", r.Key)
				return
			}

			var t tokenpb.TokenEvent
			if err := proto.Unmarshal(r.Value, &t); err != nil {
				slog.Debug("invalid protocol buffer for token event", "error", err)
				return
			}

			switch t.GetType() {
			case tokenpb.EventType_EVENT_TYPE_CREATED:
				ts.Put(token.LandscapeToken{ID: string(r.Key), Secret: t.GetToken().GetSecret()})
				slog.Debug("received token create event", "tokenID", t.GetToken().GetId(), "tokenSecret", t.GetToken().GetSecret())
			case tokenpb.EventType_EVENT_TYPE_DELETED:
				ts.Delete(string(r.Key))
				slog.Debug("received token delete event", "tokenID", r.Key)
			default:
				slog.Warn("received unhandled token event type", "eventType", tokenpb.EventType_name[int32(t.GetType())])
			}
		})
	}
}
