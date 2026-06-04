package net.explorviz.trace.extensions

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.proto.common.v1.AnyValue
import io.opentelemetry.proto.resource.v1.Resource

fun Resource.getAttributeValue(attribute: AttributeKey<String>): AnyValue? {
    return attributesList.firstOrNull { it.key == attribute.key }?.value
}
