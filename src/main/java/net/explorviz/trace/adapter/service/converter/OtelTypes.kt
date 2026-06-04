package net.explorviz.trace.adapter.service.converter

import io.opentelemetry.proto.common.v1.AnyValue
import io.opentelemetry.proto.common.v1.InstrumentationScope
import io.opentelemetry.proto.resource.v1.Resource
import io.opentelemetry.proto.trace.v1.Span
import kotlin.String

/** Represents an OpenTelemetry span. Allows accessing attributes as a [Map] and includes references to the associated
 * [OtelResource] and [OtelInstrumentationScope]
 */
data class OtelSpan(
    val spanId: String,
    val parentSpanId: String,
    val traceId: String,
    val name: String,
    val startTimeUnixNano: Long,
    val endTimeUnixNano: Long,
    val kind: OtelSpanKind,
    val events: List<OtelSpanEvent>,
    val attributes: Map<String, Any>,
    val resource: OtelResource,
    val scope: OtelInstrumentationScope
) {
    companion object {
        fun fromProto(span: Span, scope: InstrumentationScope, resource: Resource) = OtelSpan(
            spanId = span.spanId.toString(),
            parentSpanId = span.parentSpanId.toString(),
            traceId = span.traceId.toString(),
            name = span.name,
            startTimeUnixNano = span.startTimeUnixNano,
            endTimeUnixNano = span.endTimeUnixNano,
            kind = OtelSpanKind.fromProto(span.kind),
            events = span.eventsList.map(OtelSpanEvent::fromProto),
            attributes = span.attributesList.associate { it.key to it.value },
            resource = OtelResource.fromProto(resource),
            scope = OtelInstrumentationScope.fromProto(scope),

            )
    }
}

data class OtelResource(val attributes: Map<String, Any>) {
    companion object {
        fun fromProto(resource: Resource) = OtelResource(resource.attributesList.associate { it.key to it.value })
    }
}

data class OtelInstrumentationScope(val name: String, val version: String, val attributes: Map<String, Any>) {
    companion object {
        fun fromProto(scope: InstrumentationScope) = OtelInstrumentationScope(
            scope.name,
            scope.version,
            scope.attributesList.associate { it.key to it.value },
        )
    }
}

data class OtelSpanEvent(val name: String, val timeUnixNano: Long, val attributes: Map<String, Any>) {
    companion object {
        fun fromProto(spanEvent: Span.Event) = OtelSpanEvent(
            spanEvent.name,
            spanEvent.timeUnixNano,
            spanEvent.attributesList.associate { it.key to it.value },
        )
    }
}

enum class OtelSpanKind {
    SPAN_KIND_UNSPECIFIED,
    SPAN_KIND_INTERNAL,
    SPAN_KIND_SERVER,
    SPAN_KIND_CLIENT,
    SPAN_KIND_PRODUCER,
    SPAN_KIND_CONSUMER,
    UNRECOGNIZED;

    companion object {
        fun fromProto(spanKind: Span.SpanKind) = when (spanKind) {
            Span.SpanKind.SPAN_KIND_UNSPECIFIED -> SPAN_KIND_UNSPECIFIED
            Span.SpanKind.SPAN_KIND_INTERNAL -> SPAN_KIND_INTERNAL
            Span.SpanKind.SPAN_KIND_SERVER -> SPAN_KIND_SERVER
            Span.SpanKind.SPAN_KIND_CLIENT -> SPAN_KIND_CLIENT
            Span.SpanKind.SPAN_KIND_PRODUCER -> SPAN_KIND_PRODUCER
            Span.SpanKind.SPAN_KIND_CONSUMER -> SPAN_KIND_CONSUMER
            Span.SpanKind.UNRECOGNIZED -> UNRECOGNIZED
        }
    }
}

sealed interface AttributeValue {
    data class StringValue(val value: String) : AttributeValue
    data class LongValue(val value: Long) : AttributeValue
    data class DoubleValue(val value: Double) : AttributeValue
    data class BooleanValue(val value: Boolean) : AttributeValue
    data class BytesValue(val value: ByteArray) : AttributeValue
    data class ArrayValue(val value: List<AttributeValue>) : AttributeValue
    data class ObjectValue(val value: Map<String, AttributeValue>) : AttributeValue
    data object NullValue : AttributeValue

    companion object {
        fun fromProto(anyValue: AnyValue): AttributeValue {
            return when (anyValue.valueCase) {
                AnyValue.ValueCase.STRING_VALUE -> StringValue(anyValue.stringValue)
                AnyValue.ValueCase.INT_VALUE -> LongValue(anyValue.intValue)
                AnyValue.ValueCase.DOUBLE_VALUE -> DoubleValue(anyValue.doubleValue)
                AnyValue.ValueCase.BOOL_VALUE -> BooleanValue(anyValue.boolValue)
                AnyValue.ValueCase.BYTES_VALUE -> BytesValue(anyValue.bytesValue.toByteArray())
                AnyValue.ValueCase.ARRAY_VALUE -> ArrayValue(
                    anyValue.arrayValue.valuesList.map { fromProto(it) })
                AnyValue.ValueCase.KVLIST_VALUE ->
                    ObjectValue(
                        anyValue.kvlistValue.valuesList.associate { kv ->
                            kv.key to fromProto(kv.value)
                        },
                    )
//                AnyValue.ValueCase.STRING_VALUE_STRINDEX, // TODO do we need to handle this case?
                AnyValue.ValueCase.VALUE_NOT_SET -> NullValue
            }
        }
    }
}
