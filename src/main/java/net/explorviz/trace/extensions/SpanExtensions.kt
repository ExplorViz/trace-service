package net.explorviz.trace.extensions

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.proto.common.v1.AnyValue
import io.opentelemetry.proto.trace.v1.Span

fun Span.getAttributeValue(attribute: AttributeKey<String>): AnyValue? {
    return attributesList.firstOrNull { it.key == attribute.key}?.value
}

fun Span.getAttributeValue(key: String): AnyValue? {
    return attributesList.firstOrNull { it.key == key }?.value
}
