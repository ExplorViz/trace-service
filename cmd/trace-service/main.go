package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/ExplorViz/trace-service/internal/conversion"
	"github.com/ExplorViz/trace-service/internal/conversion/parsing"
	"github.com/ExplorViz/trace-service/internal/genproto/spanpb"

	"github.com/twmb/franz-go/pkg/kgo"

	"google.golang.org/protobuf/proto"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

var (
	seedBroker  = flag.String("broker", "localhost:9092", "network endpoint of the Kafka broker to use (<hostname>:<port>)")
	inTopic     = flag.String("topic-in", "test", "Kafka topic to consume OpenTelemetry spans from")
	outTopic    = flag.String("topic-out", "explorviz.spans.out", "Kafka topic to produce parsed spans into")
	logInterval = flag.Duration("log-interval", time.Second, "interval at which received spans should be logged (e.g. \"5s\")")

	lastReceivedSpans atomic.Int64
)

func main() {
	flag.Parse()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(*seedBroker),
		kgo.DefaultProduceTopic(*outTopic),
		kgo.ConsumeTopics(*inTopic),
	)
	if err != nil {
		slog.Error("unable to initialize kgo client", "error", err)
		os.Exit(1)
	}
	defer cl.Close()

	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, os.Interrupt)
	go func() {
		<-sigs
		fmt.Println("received interrupt signal; closing client")
		cl.Close()
		<-sigs
		fmt.Println("received second interrupt; exiting")
		os.Exit(1)
	}()

	fmt.Print(`
  ______            _         __      ___
 |  ____|          | |        \ \    / (_)
 | |__  __  ___ __ | | ___  _ _\ \  / / _ ____
 |  __| \ \/ / '_ \| |/ _ \| '__\ \/ / | |_  /
 | |____ >  <| |_) | | (_) | |   \  /  | |/ /
 |______/_/\_\ .__/|_|\___/|_|    \/   |_/___|
             | |
             |_|                 trace-service

`)

	go func() {
		ticker := time.NewTicker(*logInterval)
		defer ticker.Stop()

		for range ticker.C {
			count := lastReceivedSpans.Swap(0)
			slog.Info(fmt.Sprintf("received %d spans", count))
		}
	}()

	spans := make(chan parsing.SpanReader)
	results := make(chan *spanpb.SpanData)

	workerCount := runtime.NumCPU()
	for range workerCount {
		go spanWorker(spans, results)
	}
	go producerWorker(results, cl)

	for {
		fs := cl.PollFetches(context.Background())
		fs.EachRecord(func(r *kgo.Record) {
			var req coltracepb.ExportTraceServiceRequest

			if err := proto.Unmarshal(r.Value, &req); err != nil {
				slog.Debug("invalid protocol buffer", "error", err)
				return
			}

			for _, rs := range req.ResourceSpans {
				for _, ss := range rs.ScopeSpans {
					for _, s := range ss.Spans {
						spans <- parsing.NewSpanReader(s, ss.Scope, rs.Resource)
					}
				}
			}
		})
	}
}

func spanWorker(spans <-chan parsing.SpanReader, results chan<- *spanpb.SpanData) {
	for s := range spans {
		p, err := conversion.ConvertSpan(s)
		if err != nil {
			slog.Error("failed to convert span", "error", err)
			continue
		}
		results <- toProto(p)
	}
}

func producerWorker(results <-chan *spanpb.SpanData, cl *kgo.Client) {
	for s := range results {
		out, err := proto.Marshal(s)
		if err != nil {
			slog.Error("failed to encode protobuf", "error", err)
			continue
		}
		cl.ProduceSync(context.Background(), &kgo.Record{
			Key:   []byte(s.LandscapeTokenId),
			Value: out,
		})
	}
}

func toProto(ps conversion.PersistenceSpan) *spanpb.SpanData {
	s := spanpb.SpanData{
		LandscapeTokenId:     ps.LandscapeTokenId,
		LandscapeTokenSecret: ps.LandscapeTokenSecret,

		TraceId:  ps.TraceId,
		SpanId:   ps.SpanId,
		SpanName: ps.SpanName,
		ParentId: strOrNil(ps.ParentSpanId),

		StartTime: ps.StartTime,
		EndTime:   ps.EndTime,

		ApplicationName: ps.ApplicationName,
	}

	switch e := ps.Entity.(type) {
	case parsing.CodeSpanEntity:
		s.EntityDescriptor = &spanpb.SpanData_CodeDescriptor{
			CodeDescriptor: &spanpb.CodeDescriptor{
				FilePath:      e.FilePath,
				FunctionName:  e.FuncName,
				ClassName:     strOrNil(e.ClassName),
				Language:      strOrNil(e.Language),
				GitCommitHash: strOrNil(e.GitCommitHash),
			},
		}
	}

	return &s
}

func strOrNil(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
