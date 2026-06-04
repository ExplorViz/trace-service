package net.explorviz.trace.adapter.service.parsing

import io.opentelemetry.proto.common.v1.InstrumentationScope
import io.opentelemetry.proto.resource.v1.Resource
import io.opentelemetry.proto.trace.v1.Span
import net.explorviz.trace.persistence.PersistenceSpan
import net.explorviz.trace.persistence.SpanEntity

interface SpanParser {
    sealed interface SpanParseResult<out T> {
        data class Success<T>(val value: T) : SpanParseResult<T>
        data object NotApplicable : SpanParseResult<Nothing>
    }

    /**
     * Parses a span's attributes and extracts structural information for the visualization.
     *
     * @param span The OpenTelemetry span to parse
     * @param scope The instrumentation scope to which this span belongs
     * @param resource The resource that produced the span
     * @return a [Result] containing the parsed [PersistenceSpan] on success, or a failure if the span does not provide
     *     sufficient attributes to be interpreted using this parser.
     */
    fun parse(span: Span, scope: InstrumentationScope, resource: Resource): SpanParseResult<SpanEntity>
}
