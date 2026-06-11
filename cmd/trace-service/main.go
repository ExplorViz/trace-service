package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"strings"

	"github.com/ExplorViz/trace-service/internal/conversion"
	"github.com/ExplorViz/trace-service/internal/conversion/parsing"

	"github.com/twmb/franz-go/pkg/kgo"

	"google.golang.org/protobuf/proto"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

type SpanRequest struct {
	Span     *tracepb.Span
	Scope    *commonpb.InstrumentationScope
	Resource *resourcepb.Resource
}

var (
	seedBrokers = flag.String("brokers", "localhost:9092", "comma delimited list of seed brokers")
	topic       = flag.String("topic", "test", "topic to produce to and consume from")
	registry    = flag.String("registry", "localhost:8081", "schema registry port to talk to")
)

func main() {
	// TODO schema registry

	// rcl, err := sr.NewClient(sr.URLs(*registry))
	// if err != nil {
	// 	slog.Error("unable to create schema registry client", "error", err)
	// 	os.Exit(1)
	// }
	// ss, err := rcl.CreateSchema(context.Background(), *topic+"-value", sr.Schema{
	// 	Schema: schemaText,
	// 	Type:   sr.TypeProtobuf,
	// })
	// slog.Info("created or reusing schema", "subject", ss.Subject, "version", ss.Version, "id", ss.ID)

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*seedBrokers, ",")...),
		kgo.DefaultProduceTopic(*topic),
		kgo.ConsumeTopics(*topic),
	)
	if err != nil {
		slog.Error("unable to initialize kgo client", "error", err)
		os.Exit(1)
	}

	spans := make(chan parsing.SpanReader)
	results := make(chan conversion.PersistenceSpan)

	workerCount := runtime.NumCPU()
	for range workerCount {
		go spanWorker(spans, results, cl)
	}

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

func spanWorker(spans <-chan parsing.SpanReader, results chan<- conversion.PersistenceSpan, cl *kgo.Client) {
	for s := range spans {
		p, err := conversion.ConvertSpan(s)
		if err != nil {
			slog.Error(fmt.Sprintf("failed to convert span: %s", err))
			continue
		}
		results <- p
	}
}

func producerWorker(results <-chan conversion.PersistenceSpan, cl *kgo.Client) {
	for p := range results {

	}
}
