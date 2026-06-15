package conversion

import (
	"github.com/ExplorViz/trace-service/internal/genproto/spanpb"
	"github.com/ExplorViz/trace-service/internal/parsing"
)

func ToProto(ps ParsedSpan) *spanpb.ParsedSpan {
	s := spanpb.ParsedSpan{
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
		s.EntityDescriptor = &spanpb.ParsedSpan_CodeDescriptor{
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
