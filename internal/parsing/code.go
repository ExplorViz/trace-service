package parsing

import (
	"cmp"
	"errors"
	"fmt"

	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"

	"github.com/ExplorViz/trace-service/internal/attrib"
)

type CodeSpanEntity struct {
	FilePath      string
	FuncName      string
	ClassName     string
	Language      string
	GitCommitHash string
}

func (c CodeSpanEntity) Id() string {
	return c.FilePath + " " + c.FuncName + " " + c.ClassName + " " + c.GitCommitHash
}

func ParseCodeSpan(s *attrib.SpanReader) (SpanEntity, error) {
	fqn := s.SpanAttribute(semconv.CodeFunctionNameKey).GetStringValue()
	lang := s.ResourceAttribute(semconv.TelemetrySDKLanguageKey).GetStringValue()

	parsedFqn := ParseFunctionFqn(fqn, lang)

	if parsedFqn.FuncName == "" {
		return &CodeSpanEntity{}, errors.New("code parser: function name could not be extracted")
	}

	filePath := cmp.Or(parsedFqn.FilePath, s.SpanAttribute(semconv.CodeFilePathKey).GetStringValue())

	if filePath == "" {
		return CodeSpanEntity{}, fmt.Errorf("code parser: file path could not be extracted from FQN and %s not given", semconv.CodeFilePathKey)
	}

	gitHash := s.SpanAttribute(semconv.VCSRefHeadRevisionKey).GetStringValue()

	return CodeSpanEntity{
		FilePath:      filePath,
		FuncName:      parsedFqn.FuncName,
		ClassName:     parsedFqn.ClassName,
		Language:      lang,
		GitCommitHash: gitHash}, nil
}
