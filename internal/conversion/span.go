package conversion

import (
	"errors"
	"log/slog"

	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"

	"github.com/ExplorViz/trace-service/internal/attrib"
	"github.com/ExplorViz/trace-service/internal/parsing"
)

type ParsedSpan struct {
	LandscapeTokenId     string
	LandscapeTokenSecret string

	TraceId      string
	SpanId       string
	SpanName     string
	ParentSpanId string

	StartTime uint64
	EndTime   uint64

	ApplicationName string
	Entity          parsing.SpanEntity
}

var parserChain = []parsing.SpanParser{
	parsing.ParseCodeSpan,
}

func ConvertSpan(sr *attrib.SpanReader) (ParsedSpan, error) {
	tokenId := sr.ResourceAttribute(attrib.ExplorVizAttributes.LandscapeTokenID.Key).GetStringValue()
	tokenSecret := sr.ResourceAttribute(attrib.ExplorVizAttributes.LandscapeTokenSecret.Key).GetStringValue()
	appName := sr.ResourceAttribute(semconv.ServiceNameKey).GetStringValue()

	for _, parser := range parserChain {
		res, err := parser(sr)
		if err != nil {
			slog.Debug("parser failed", "error", err)
			continue
		}
		return ParsedSpan{
			LandscapeTokenId:     tokenId,
			LandscapeTokenSecret: tokenSecret,

			TraceId:      string(sr.Span.GetTraceId()),
			SpanId:       string(sr.Span.GetSpanId()),
			ParentSpanId: string(sr.Span.GetParentSpanId()),

			StartTime: sr.Span.GetStartTimeUnixNano(),
			EndTime:   sr.Span.GetEndTimeUnixNano(),

			ApplicationName: appName,
			Entity:          res,
		}, nil
	}
	return ParsedSpan{}, errors.New("no matching span parser found")
}
