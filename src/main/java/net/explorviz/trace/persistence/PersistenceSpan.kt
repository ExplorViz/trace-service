package net.explorviz.trace.persistence

data class PersistenceSpan(
    val landscapeTokenId: String,
    val landscapeTokenSecret: String,

    val traceId: String?,
    val spanId: String?,
    val parentSpanId: String?,

    val startTime: Long,
    val endTime: Long,

    val applicationName: String,

    val spanEntity: SpanEntity,
)
