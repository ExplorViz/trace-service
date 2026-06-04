package net.explorviz.trace.adapter.service.converter

import io.opentelemetry.proto.common.v1.InstrumentationScope
import io.opentelemetry.proto.resource.v1.Resource
import io.opentelemetry.proto.trace.v1.Span
import io.opentelemetry.semconv.ServiceAttributes
import jakarta.enterprise.context.ApplicationScoped
import java.util.*
import net.explorviz.trace.adapter.service.parsing.CodeSpanParser
import net.explorviz.trace.adapter.service.parsing.SpanParser
import net.explorviz.trace.adapter.service.parsing.SpanParsingException
import net.explorviz.trace.attributes.ExplorvizAttributes
import net.explorviz.trace.extensions.getAttributeValue
import net.explorviz.trace.persistence.PersistenceSpan
import net.explorviz.trace.persistence.SpanEntity

/** Converts a [io.opentelemetry.proto.trace.v1.Span] to a [PersistenceSpan]. */
@ApplicationScoped
class SpanConverterImpl : SpanConverter<PersistenceSpan> {
    companion object {
        val parsers = arrayOf(
            CodeSpanParser(),
        )
    }

    override fun fromOpenTelemetrySpan(
        span: Span, scope: InstrumentationScope, resource: Resource
    ): Result<PersistenceSpan> {
        val landscapeTokenId: String = span.getAttributeValue(ExplorvizAttributes.TOKEN_ID.key)?.stringValue
            ?: ExplorvizAttributes.TOKEN_ID.defaultValue

        val landscapeTokenSecret: String = span.getAttributeValue(ExplorvizAttributes.TOKEN_SECRET.key)?.stringValue
            ?: ExplorvizAttributes.TOKEN_SECRET.defaultValue

        val traceId: String = IdHelper.convertTraceId(span.traceId.toByteArray())
        val spanId: String = IdHelper.convertSpanId(span.spanId.toByteArray())
        val parentSpanId: String? =
            span.parentSpanId.takeIf { it.size() > 0 }?.toByteArray()?.let(IdHelper::convertSpanId)

        val startTime: Long = span.startTimeUnixNano
        val endTime: Long = span.endTimeUnixNano

        if (startTime > endTime) {
            return Result.failure(SpanParsingException("Span start time must not exceed end time"))
        }

        val serviceName: String =
            resource.getAttributeValue(ServiceAttributes.SERVICE_NAME)?.stringValue ?: "Unknown Service"

        val spanEntity: SpanEntity = parsers
            .asSequence()
            .firstNotNullOfOrNull {
                when (val result = it.parse(span, scope, resource)) {
                    is SpanParser.SpanParseResult.Success -> result.value
                    else -> null
                }
            } ?: return Result.failure(SpanParsingException("No suitable parser found"))

        return Result.success(
            PersistenceSpan(
                landscapeTokenId,
                landscapeTokenSecret,
                traceId,
                spanId,
                parentSpanId,
                startTime,
                endTime,
                serviceName,
                spanEntity,
            ),
        )
    }
}
