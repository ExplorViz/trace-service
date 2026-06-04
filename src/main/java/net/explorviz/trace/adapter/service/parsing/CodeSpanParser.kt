package net.explorviz.trace.adapter.service.parsing

import io.opentelemetry.proto.common.v1.InstrumentationScope
import io.opentelemetry.proto.resource.v1.Resource
import io.opentelemetry.proto.trace.v1.Span
import io.opentelemetry.semconv.CodeAttributes
import io.opentelemetry.semconv.TelemetryAttributes
import io.opentelemetry.semconv.incubating.VcsIncubatingAttributes
import io.quarkus.logging.Log
import net.explorviz.trace.adapter.service.parsing.fqn.DefaultFqnParser
import net.explorviz.trace.adapter.service.parsing.fqn.FqnParser
import net.explorviz.trace.adapter.service.parsing.fqn.JavaFqnParser
import net.explorviz.trace.extensions.getAttributeValue
import net.explorviz.trace.persistence.CodeSpanEntity
import net.explorviz.trace.persistence.SpanEntity

/**
 * Parses spans representing function calls. A span can be successfully parsed by this parser iff both the function name
 * and the corresponding file name can be extracted from the span's attributes. This parser relies on the Semantic
 * Convention's [Code Attributes](https://opentelemetry.io/docs/specs/semconv/registry/attributes/code/).
 */
class CodeSpanParser : SpanParser {
    companion object {
        private val JAVA_PARSER = JavaFqnParser()
        private val DEFAULT_PARSER = DefaultFqnParser()

        fun getFqnParser(language: String?): FqnParser {
            return when (language?.lowercase()) {
                "java" -> JAVA_PARSER
                else -> DEFAULT_PARSER
            }
        }
    }

    override fun parse(
        span: Span, scope: InstrumentationScope, resource: Resource
    ): SpanParser.SpanParseResult<SpanEntity> {
        val codeFunctionName: String = span.getAttributeValue(CodeAttributes.CODE_FUNCTION_NAME)?.stringValue ?: run {
            Log.trace("Code parsing failed: Missing attribute ${CodeAttributes.CODE_FUNCTION_NAME}")
            return SpanParser.SpanParseResult.NotApplicable
        }

        val language: String? = resource.getAttributeValue(TelemetryAttributes.TELEMETRY_SDK_LANGUAGE)?.stringValue

        val parsedFqn: FqnParser.FqnParseResult = getFqnParser(language).parseFunctionFqn(codeFunctionName)

        // FQN parsing result takes precedence. If unset (i.e. only a function name is given), then attempt to use
        // explicit file path attribute as fallback. Note that the semantic conventions recommend using an absolute path
        // for this attribute, which for the purposes of our visualization is not desirable.
        val filePath: String? =
            parsedFqn.filePath.ifBlank { span.getAttributeValue(CodeAttributes.CODE_FILE_PATH)?.stringValue }

        if (parsedFqn.functionName.isBlank()) {
            Log.trace("Code parsing failed: Function name could not be extracted")
            return SpanParser.SpanParseResult.NotApplicable
        }

        if (filePath.isNullOrBlank()) {
            Log.trace("Code parsing failed: File path could not be extracted and ${CodeAttributes.CODE_FILE_PATH} not given")
            return SpanParser.SpanParseResult.NotApplicable
        }

        val gitCommitHash: String? = span.getAttributeValue(VcsIncubatingAttributes.VCS_REF_HEAD_REVISION)?.stringValue

        return SpanParser.SpanParseResult.Success(
            CodeSpanEntity(
                filePath,
                parsedFqn.functionName,
                parsedFqn.className,
                language,
                gitCommitHash,
            ),
        )
    }
}
