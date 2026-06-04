package net.explorviz.trace.adapter.service.converter

import io.opentelemetry.proto.common.v1.InstrumentationScope
import io.opentelemetry.proto.resource.v1.Resource
import io.opentelemetry.proto.trace.v1.Span

/**
 * Converts OpenTelemetry spans into another type.
 *
 * @param <T> the type spans are converted to.
 */
interface SpanConverter<T> {
    /** Converts an OpenTelemetry {@link Span} into {@link T}. */
    fun fromOpenTelemetrySpan(span: Span, scope: InstrumentationScope, resource: Resource): Result<T>
}
