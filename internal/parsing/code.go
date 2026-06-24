package parsing

import (
	"cmp"
	"errors"
	"fmt"

	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"

	"github.com/ExplorViz/trace-service/internal/attrib"
)

// A CodeSpanEntity represents the execution of a function.
type CodeSpanEntity struct {
	// FilePath is the path of the file within which the function is contained, with "/" as the separator.
	FilePath string

	// FuncName is the name of the executed function, excluding its signature.
	FuncName string

	// ClassName is the qualified name of the class within which the function is contained (if any).
	// Inner classes should be separated against containing classes using ".",e.g. ("OuterClass.InnerClass").
	ClassName string

	// Language specifies the programming language runtime of the executed function. If applicable, the format
	// should match the well-known values specified by OpenTelemetry's telemetry.sdk.language attribute.
	Language string

	// GitCommitHash can be specified if the function is contained within a file at a known commit.
	// This can be used to correlate data from runtime analysis with static analysis data.
	GitCommitHash string
}

func (c CodeSpanEntity) Id() string {
	return c.FilePath + " " + c.FuncName + " " + c.ClassName + " " + c.GitCommitHash
}

// ParseCodeSpan parses spans describing function executions by looking for attributes conforming to the
// [OTel semconv code attributes]. For a span to be successfully parsed, it needs to provide:
//   - a relative path of the file containing the function, ideally relative to the repository root
//   - the name of the executed function
//
// Since relative paths for the file are preferred, it first attempts to parse the function fully-qualified
// name (FQN) for file path information. Only if this is insufficient will it look for an explicit file path.
//
// [OTel semconv code attributes]: https://opentelemetry.io/docs/specs/semconv/registry/attributes/code/
func ParseCodeSpan(sr *attrib.SpanReader) (SpanEntity, error) {
	fqn := sr.SpanAttribute(semconv.CodeFunctionNameKey).GetStringValue()
	lang := sr.ResourceAttribute(semconv.TelemetrySDKLanguageKey).GetStringValue()

	parsedFQN := ParseFunctionFQN(fqn, lang)

	if parsedFQN.FuncName == "" {
		return &CodeSpanEntity{}, errors.New("code parser: function name could not be extracted")
	}

	filePath := cmp.Or(parsedFQN.FilePath, sr.SpanAttribute(semconv.CodeFilePathKey).GetStringValue())

	if filePath == "" {
		return CodeSpanEntity{}, fmt.Errorf("code parser: file path could not be extracted from FQN and %s not given", semconv.CodeFilePathKey)
	}

	gitHash := sr.SpanAttribute(semconv.VCSRefHeadRevisionKey).GetStringValue()

	return CodeSpanEntity{
		FilePath:      filePath,
		FuncName:      parsedFQN.FuncName,
		ClassName:     parsedFQN.ClassName,
		Language:      lang,
		GitCommitHash: gitHash}, nil
}
