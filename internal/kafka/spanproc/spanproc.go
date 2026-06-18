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

func Run(ctx context.Context, cl *kgo.Client, validateTokens bool, ts *tokenproc.TokenStore, logInterval time.Duration) {
	spans := make(chan *attrib.SpanReader)
	results := make(chan *spanpb.ParsedSpan)

	workerCount := runtime.NumCPU()
	var wg sync.WaitGroup
	for range workerCount {
		wg.Go(func() { consumerWorker(ctx, spans, results, ts, validateTokens) })
	}
	wg.Go(func() { producerWorker(ctx, results, cl) })

	if logInterval > 0 {
		go func() {
			ticker := time.NewTicker(logInterval)
			defer ticker.Stop()

			for range ticker.C {
				count := lastReceivedSpans.Swap(0)
				slog.Info(fmt.Sprintf("received %d spans", count))
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

func consumerWorker(ctx context.Context, spans <-chan *attrib.SpanReader, results chan<- *spanpb.ParsedSpan, ts *tokenproc.TokenStore, validateTokens bool) {
	for {
		select {
		case <-ctx.Done():
			slog.Debug("exiting span consumer worker")
			return

		case s := <-spans:
			lastReceivedSpans.Add(1)

			if validateTokens {
				tokenID := s.SpanAttribute(attrib.ExplorVizAttributes.LandscapeTokenID.Key).GetStringValue()
				tokenSecret := s.SpanAttribute(attrib.ExplorVizAttributes.LandscapeTokenSecret.Key).GetStringValue()
				if !ts.HasToken(tokenID, tokenSecret) {
					slog.Debug("invalid span: unknown landscape token ID or incorrect secret", "landscapeTokenID", tokenID)
					lastInvalidSpans.Add(1)
					continue
				}
			}

			p, err := conversion.ConvertSpan(s)
			if err != nil {
				slog.Error("failed to convert span", "error", err)
				continue
			}
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
		}
	}
}
