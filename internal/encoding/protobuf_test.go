package encoding

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ExplorViz/trace-service/internal/parsing"
)

func TestToProtobuf(t *testing.T) {
	s := parsing.ParsedSpan{
		LandscapeTokenID:     "mytokenvalue",
		LandscapeTokenSecret: "mytokensecret",

		TraceID:      "5b8aa5a2d2c872e8321cf37308d69df2",
		SpanID:       "5fb397be34d26b51",
		SpanName:     "hello-greetings",
		ParentSpanID: "051581bf3cb55c13",

		StartTime: 0,
		EndTime:   10,

		ApplicationName: "trace-service",
		Entity: parsing.CodeSpanEntity{
			FilePath: "net/explorviz/example/MyClass.java",
			FuncName: "complexCalculation",
		},
	}
	pb, err := ToProtobuf(s)
	assert.NoError(t, err)
	assert.NotNil(t, pb)

	assert.Equal(t, s.LandscapeTokenID, pb.GetLandscapeTokenId())
	assert.Equal(t, s.LandscapeTokenSecret, pb.GetLandscapeTokenSecret())
	assert.Equal(t, s.TraceID, pb.GetTraceId())
	assert.Equal(t, s.SpanID, pb.GetSpanId())
	assert.Equal(t, s.ParentSpanID, pb.GetParentId())
	assert.Equal(t, s.StartTime, pb.StartTime)
	assert.Equal(t, s.EndTime, pb.EndTime)
	assert.Equal(t, s.ApplicationName, pb.ApplicationName)

	assert.NotEmpty(t, pb.EntityId)
	assert.Equal(t, s.Entity.Id(), pb.EntityId)
}

func TestToProtobufNilFields(t *testing.T) {
	s := parsing.ParsedSpan{
		LandscapeTokenID:     "mytokenvalue",
		LandscapeTokenSecret: "mytokensecret",

		TraceID:  "5b8aa5a2d2c872e8321cf37308d69df2",
		SpanID:   "5fb397be34d26b51",
		SpanName: "hello-greetings",

		StartTime: 0,
		EndTime:   10,

		ApplicationName: "trace-service",
		Entity: parsing.CodeSpanEntity{
			FilePath: "net/explorviz/example/MyClass.java",
			FuncName: "complexCalculation",
		},
	}
	pb, err := ToProtobuf(s)
	assert.NoError(t, err)
	assert.NotNil(t, pb)

	assert.Nil(t, pb.ParentId)
}

func TestToProtobufCode(t *testing.T) {
	ce := parsing.CodeSpanEntity{
		FilePath:      "net/explorviz/example/MyClass.java",
		FuncName:      "complexCalculation",
		ClassName:     "MyClass",
		Language:      "java",
		GitCommitHash: "18c5b488a3b2e218c0e0cf2a7d4820d9da93a554",
	}
	pb, err := ToProtobuf(parsing.ParsedSpan{
		Entity: ce,
	})
	desc := pb.GetCodeDescriptor()
	assert.NoError(t, err)
	assert.NotNil(t, pb)
	assert.NotNil(t, desc)

	assert.Equal(t, ce.FilePath, desc.GetFilePath())
	assert.Equal(t, ce.FuncName, desc.GetFunctionName())
	assert.Equal(t, ce.ClassName, desc.GetClassName())
	assert.Equal(t, ce.Language, desc.GetLanguage())
	assert.Equal(t, ce.GitCommitHash, desc.GetGitCommitHash())
}

func TestToProtobufCodeNilFields(t *testing.T) {
	ce := parsing.CodeSpanEntity{
		FilePath: "net/explorviz/example/MyClass.java",
		FuncName: "complexCalculation",
	}
	pb, err := ToProtobuf(parsing.ParsedSpan{
		Entity: ce,
	})
	desc := pb.GetCodeDescriptor()
	assert.NoError(t, err)
	assert.NotNil(t, pb)
	assert.NotNil(t, desc)

	assert.Nil(t, desc.ClassName)
	assert.Nil(t, desc.Language)
	assert.Nil(t, desc.GitCommitHash)
}
