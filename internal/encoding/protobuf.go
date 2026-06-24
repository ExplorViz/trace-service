// Package encoding provides functions for serializing domain objects for network transmission.
package encoding

import (
	"fmt"

	"github.com/ExplorViz/trace-service/internal/genproto/spanpb"
	"github.com/ExplorViz/trace-service/internal/parsing"
)

// ToProtobuf maps a [ParsedSpan] to its Protobuf representation [spanpb.ParsedSpan].
// Returns an error if the provided [parsing.SpanEntity] is nil or if the entity type is not known.
func ToProtobuf(ps parsing.ParsedSpan) (*spanpb.ParsedSpan, error) {
	if ps.Entity == nil {
		return nil, fmt.Errorf("protobuf conversion: encountered nil entity")
	}

	s := spanpb.ParsedSpan{
		LandscapeTokenId:     ps.LandscapeTokenID,
		LandscapeTokenSecret: ps.LandscapeTokenSecret,

		TraceId:  ps.TraceID,
		SpanId:   ps.SpanID,
		SpanName: ps.SpanName,
		ParentId: strOrNil(ps.ParentSpanID),

		StartTime: ps.StartTime,
		EndTime:   ps.EndTime,

		ApplicationName: ps.ApplicationName,

		EntityId: ps.Entity.Id(),
	}

	switch e := ps.Entity.(type) {
	case parsing.CodeSpanEntity:
		s.EntityDescriptor = &spanpb.ParsedSpan_CodeDescriptor{
			CodeDescriptor: &spanpb.CodeDescriptor{
				FilePath:      e.FilePath,
				FunctionName:  e.FuncName,
				ClassName:     strOrNil(e.ClassName),
				Language:      strOrNil(e.Language),
				GitCommitHash: strOrNil(e.GitCommitHash),
			},
		}
	default:
		return nil, fmt.Errorf("protobuf conversion: encountered unhandled span entity type")
	}

	return &s, nil
}

func strOrNil(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
