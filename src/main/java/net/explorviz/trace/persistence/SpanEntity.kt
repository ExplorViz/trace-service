package net.explorviz.trace.persistence

sealed interface SpanEntity

data class CodeSpanEntity(
    val filePath: String,
    val functionName: String,
    val className: String? = null,
    val language: String? = null,
    val gitCommitHash: String? = null
) : SpanEntity
