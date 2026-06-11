package conversion

import (
	"errors"
	"log/slog"

	"github.com/ExplorViz/trace-service/internal/conversion/parsing"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
)

type ExplorVizAttribute struct {
	Key          attribute.Key
	DefaultValue string
}

var ExplorVizAttributes = struct {
	LandscapeTokenID     ExplorVizAttribute
	LandscapeTokenSecret ExplorVizAttribute
	EntityId             ExplorVizAttribute
}{
	LandscapeTokenID: ExplorVizAttribute{
		Key:          "explorviz.token.id",
		DefaultValue: "mytokenvalue",
	},
	LandscapeTokenSecret: ExplorVizAttribute{
		Key:          "explorviz.token.secret",
		DefaultValue: "mytokenvalue",
	},
	EntityId: ExplorVizAttribute{
		Key:          "explorviz.entity.id",
		DefaultValue: "unknown",
	},
}

type PersistenceSpan struct {
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

func ConvertSpan(sr parsing.SpanReader) (PersistenceSpan, error) {
	tokenId := sr.ResourceAttribute(ExplorVizAttributes.LandscapeTokenID.Key).GetStringValue()
	tokenSecret := sr.ResourceAttribute(ExplorVizAttributes.LandscapeTokenSecret.Key).GetStringValue()
	appName := sr.ResourceAttribute(semconv.ServiceNameKey).GetStringValue()

	for _, parser := range parserChain {
		res, err := parser(sr)
		if err != nil {
			slog.Debug("parser failed", "error", err)
			continue
		}
		return PersistenceSpan{
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
	return PersistenceSpan{}, errors.New("no matching span parser found")
}
