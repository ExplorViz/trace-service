package spanproc

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"github.com/ExplorViz/trace-service/internal/attrib"
	"github.com/ExplorViz/trace-service/internal/conversion"
	"github.com/ExplorViz/trace-service/internal/genproto/spanpb"
	"github.com/ExplorViz/trace-service/internal/kafka/tokenproc"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

var lastReceivedSpans atomic.Uint64
var lastInvalidSpans atomic.Uint64
var lastExportedSpans atomic.Uint64
var lastKnownSpans atomic.Uint64

type spanCache struct {
	mu sync.Mutex
	m  map[string]map[string]bool
}

func newSpanCache() spanCache {
	return spanCache{
		m: make(map[string]map[string]bool),
	}
}

func (s *spanCache) add(landscapeTokenID string, spanID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.m[landscapeTokenID][spanID] = true
}

func (s *spanCache) contains(landscapeTokenID string, spanID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	m, ok := s.m[landscapeTokenID]
	if !ok {
		return false
	}

	_, ok = m[spanID]
	return ok
}

func Run(ctx context.Context, cl *kgo.Client, validateTokens bool, ts *tokenproc.TokenStore, logInterval time.Duration) {
	spans := make(chan *attrib.SpanReader)
	results := make(chan *spanpb.ParsedSpan)
	knownSpans := newSpanCache()

	workerCount := runtime.NumCPU()
	var wg sync.WaitGroup
	for range workerCount {
		wg.Go(func() { consumerWorker(ctx, spans, results, &knownSpans, ts, validateTokens) })
	}
	wg.Go(func() { producerWorker(ctx, results, cl) })

	if logInterval > 0 {
		go func() {
			ticker := time.NewTicker(logInterval)
			defer ticker.Stop()

			for range ticker.C {
				slog.Info(
					"span log interval",
					"received", lastReceivedSpans.Swap(0),
					"invalid", lastInvalidSpans.Swap(0),
					"exported", lastExportedSpans.Swap(0),
					"known", lastKnownSpans.Swap(0))
			}
		}()
	}

	for {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			slog.Debug("exiting kafka span poll loop")
			break
		}
		fs.EachRecord(func(r *kgo.Record) {
			var req coltracepb.ExportTraceServiceRequest
			if err := proto.Unmarshal(r.Value, &req); err != nil {
				slog.Debug("invalid protocol buffer for span", "error", err)
				return
			}

			for _, rs := range req.GetResourceSpans() {
				for _, ss := range rs.GetScopeSpans() {
					for _, s := range ss.GetSpans() {
						sr := attrib.NewSpanReader(s, ss.GetScope(), rs.GetResource())
						spans <- &sr
					}
				}
			}
		})
	}

	wg.Wait()
}

func consumerWorker(ctx context.Context, spans <-chan *attrib.SpanReader, results chan<- *spanpb.ParsedSpan, sc *spanCache, ts *tokenproc.TokenStore, validateTokens bool) {
	for {
		select {
		case <-ctx.Done():
			slog.Debug("exiting span consumer worker")
			return

		case sr := <-spans:
			lastReceivedSpans.Add(1)

			if err := validate(sr, ts, validateTokens); err != nil {
				lastInvalidSpans.Add(1)
				slog.Debug("received invalid span", "error", err)
			}

			tokenID := sr.TokenID()
			if sc.contains(tokenID, string(sr.Span.SpanId)) {
				lastKnownSpans.Add(1)
				slog.Debug("received already known span", "spanID", sr.Span.SpanId)
				continue
			}

			p, err := conversion.ConvertSpan(sr)
			if err != nil {
				slog.Error("failed to convert span", "error", err)
				continue
			}
			sc.add(tokenID, string(sr.Span.SpanId))
			results <- conversion.ToProto(p)
		}
	}
}

func producerWorker(ctx context.Context, results <-chan *spanpb.ParsedSpan, cl *kgo.Client) {
	for {
		select {
		case <-ctx.Done():
			slog.Debug("exiting span producer worker")
			return

		case s := <-results:
			out, err := proto.Marshal(s)
			if err != nil {
				slog.Error("failed to encode protobuf", "error", err)
				continue
			}
			cl.ProduceSync(ctx, &kgo.Record{
				Key:   []byte(s.GetLandscapeTokenId()),
				Value: out,
			})
			lastExportedSpans.Add(1)
		}
	}
}

func validate(sr *attrib.SpanReader, ts *tokenproc.TokenStore, validateTokens bool) error {
	id := sr.TokenID()
	secret := sr.TokenSecret()

	if id == "" || secret == "" {
		return fmt.Errorf("missing landscape token ID or secret attribute")
	}

	if validateTokens {
		if !ts.HasToken(id, secret) {
			return fmt.Errorf("unknown landscape token ID \"%s\" or incorrect secret", id)
		}
	}

	return nil
}
