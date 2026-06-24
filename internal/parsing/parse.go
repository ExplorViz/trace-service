// Package parsing is concerned with the interpretation of span attributes,
// with the goal of infering the entity within the system that the span describes.
//
// Various parsing functions and result types are defined which rely on different
// attributes from the [OTel semantic conventions].
//
// [OTel semantic conventions]: https://opentelemetry.io/docs/specs/semconv/registry/attributes/code/
package parsing

import (
	"errors"
	"log/slog"

	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"

	"github.com/ExplorViz/trace-service/internal/attrib"
)

// A ParsedSpan represents a span which has been parsed to extract the underlying entity
// which the span describes. Next to regular span properties, it provides fields for
// matching the span to an ExplorViz landscape, as well as a description of the extracted
// entity in [ParsedSpan.Entity].
type ParsedSpan struct {
	LandscapeTokenID     string
	LandscapeTokenSecret string

	TraceID      string
	SpanID       string
	SpanName     string
	ParentSpanID string

	StartTime uint64
	EndTime   uint64

	ApplicationName string
	Entity          SpanEntity
}

// A SpanParser is a function which extracts a specific type of entity described by spans.
// Such a function has specific attributes it looks for within spans, and if sufficient information
// is given to reliably classify the entity described by the span, a [SpanEntity] is returned.
// Otherwise, an error is returned.
type SpanParser func(sr *attrib.SpanReader) (SpanEntity, error)

// A SpanEntity is a component of interest for which spans may be created.
// Examples include functions in code, HTTP endpoints, and databases.
type SpanEntity interface {
	Id() string
}

var parserChain = []SpanParser{
	ParseCodeSpan,
}

// ParseSpan applies a series of parsing functions to the given [*attrib.SpanReader]
// which attempt to extract a [SpanEntity] that the span describes. If insufficient
// information is provided within the attributes for a parser to extract an entity, the next
// parser in the chain is used. The first parsing function to return a non-error value gives
// the final result. If all parsing functions fail, an error is returned.
func ParseSpan(sr *attrib.SpanReader) (ParsedSpan, error) {
	appName := sr.ResourceAttribute(semconv.ServiceNameKey).GetStringValue()

	if appName == "" {
		appName = attrib.FallbackValues.ServiceName
	}

	for _, parser := range parserChain {
		entity, err := parser(sr)
		if err != nil {
			slog.Debug("parser failed", "error", err)
			continue
		}
		return ParsedSpan{
			LandscapeTokenID:     sr.TokenID(),
			LandscapeTokenSecret: sr.TokenSecret(),

			TraceID:      sr.TraceID(),
			SpanID:       sr.SpanID(),
			ParentSpanID: sr.ParentSpanID(),

			StartTime: sr.Span.GetStartTimeUnixNano(),
			EndTime:   sr.Span.GetEndTimeUnixNano(),

			ApplicationName: appName,
			Entity:          entity,
		}, nil
	}
	return ParsedSpan{}, errors.New("no matching span parser found")
}
