package parsing

import "github.com/ExplorViz/trace-service/internal/attrib"

type SpanParser func(s *attrib.SpanReader) (SpanEntity, error)

type SpanEntity interface {
	Id() string
}
