package parsing

import "github.com/ExplorViz/trace-service/internal/attrib"

type SpanParser func(sr *attrib.SpanReader) (SpanEntity, error)

type SpanEntity interface {
	Id() string
}
