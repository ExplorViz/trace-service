package parsing

import (
	"log/slog"
	"strings"
	"unicode"
)

type FqnParseResult struct {
	FilePath  string
	FuncName  string
	ClassName string
}

func ParseFunctionFqn(fqn string, language string) FqnParseResult {
	switch strings.ToLower(language) {
	case "java":
		return parseJavaFqn(fqn)
	default:
		slog.Warn("unknown or missing SDK language, using generic fqn parser", "telemetry.sdk.language", language)
		return parseGenericFqn(fqn)
	}
}

// ParseJavaFqn parses a Java method fully-qualified name (fqn) as specified by the [OTel semantic conventions].
// Attempts to detect inner classes if multiple fqn segments are capitalized.
//
// Example fqn:
//
//	"net.explorviz.app.MyConverter.MyInnerClass.convert"
//
// Example result:
//
//	["net/explorviz/app/MyConverter.java", "convert", "MyConverter.MyInnerClass"]
//
// [OTel semantic conventions]: https://opentelemetry.io/docs/specs/semconv/registry/attributes/code/
func parseJavaFqn(fqn string) FqnParseResult {
	const FileExtension = ".java"

	// Ignore lambda portion. If we explicitly want to support this, the data model would need to allow functions
	// to have child functions. For now, we simplify and treat this as a call of the containing function.
	fqnNoLambda, _, _ := strings.Cut(fqn, "$")
	separatedFqn := strings.Split(fqnNoLambda, ".")

	lastPackageIndex := -1
	for i := len(separatedFqn) - 2; i >= 0; i-- {
		if !startsWithUpper(separatedFqn[i]) {
			lastPackageIndex = i
			break
		}
	}

	fileNameIndex := lastPackageIndex + 1
	filePath := strings.Join(separatedFqn[:fileNameIndex+1], "/") + FileExtension

	className := ""
	if fileNameIndex < len(separatedFqn)-1 {
		className = strings.Join(
			separatedFqn[fileNameIndex:len(separatedFqn)-1],
			".",
		)
	}

	methodName := separatedFqn[len(separatedFqn)-1]

	return FqnParseResult{FilePath: filePath, FuncName: methodName, ClassName: className}
}

// ParseGenericFqn acts as a fallback generic parser for fully-qualified function names where the language is not known.
// Assumes dot-separated string, where the last segment represents the function name and the prior segments represent the
// file's path. Does not add any file extension to the file path. Assumes no classes are included in the fqn.
//
// Example fqn:
//
//	"src.main.SomeFile.myFunc"
//
// Example result:
//
//	["src/main/SomeFile", "myFunc", ""]
func parseGenericFqn(fqn string) FqnParseResult {
	separatedFqn := strings.Split(fqn, ".")
	return FqnParseResult{
		FilePath: strings.Join(separatedFqn[:len(separatedFqn)-1], "/"),
		FuncName: separatedFqn[len(separatedFqn)-1]}
}

func startsWithUpper(s string) bool {
	for _, r := range s {
		return unicode.IsUpper(r)
	}
	return false
}
