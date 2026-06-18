// Package tokenproc is concerned with processing landscape token events,
// such as the creation of new tokens and the deletion of existing tokens.
// It provides means of keeping track of existing landscape tokens and react
package tokenproc

import (
	"context"
	"log/slog"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"github.com/ExplorViz/trace-service/internal/genproto/tokenpb"
)

// A TokenStore keeps track of existing landscape tokens and their corresponding token secrets.
type TokenStore struct {
	mu sync.RWMutex
	m  map[string]string
}

func NewTokenStore() TokenStore {
	return TokenStore{
		m: make(map[string]string),
	}
}

func (s *TokenStore) put(id string, secret string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.m[id] = secret
}

func (s *TokenStore) delete(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.m, id)
}

// HasToken reports whether the store knows about a landscape token
// with the given ID and secret
func (s *TokenStore) HasToken(id string, secret string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	val, ok := s.m[id]
	return ok && val == secret
}

// Run continuously fetches records from the given client, attempts to deserialize them
// as [tokenpb.TokenEvent]s and updates the provided [TokenStore] accordingly.
func Run(ctx context.Context, cl *kgo.Client, ts *TokenStore) {
	for {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			slog.Debug("exiting kafka token poll loop")
			break
		}
		fs.EachRecord(func(r *kgo.Record) {
			if r.Value == nil {
				ts.delete(string(r.Key))
				slog.Debug("received token delete event", "tokenID", r.Key)
				return
			}

			var t tokenpb.TokenEvent
			if err := proto.Unmarshal(r.Value, &t); err != nil {
				slog.Debug("invalid protocol buffer for token event", "error", err)
				return
			}

			ts.put(string(r.Key), t.GetToken().GetSecret())
			slog.Debug("received token create event", "tokenID", t.GetToken().GetId(), "tokenSecret", t.GetToken().GetSecret())
		})
	}
}
