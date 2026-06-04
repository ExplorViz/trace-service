package net.explorviz.trace.messaging

import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import net.explorviz.landscape.avro.CodeDescriptor
import net.explorviz.landscape.avro.SpanData
import net.explorviz.trace.persistence.CodeSpanEntity
import net.explorviz.trace.persistence.PersistenceSpan
import net.explorviz.trace.persistence.SpanEntity
import org.eclipse.microprofile.reactive.messaging.Channel
import org.eclipse.microprofile.reactive.messaging.Emitter
import org.eclipse.microprofile.reactive.messaging.Message

@ApplicationScoped
class KafkaSpanExporter {
    @Inject @Channel("explorviz-spans") private lateinit var emitter: Emitter<SpanData>

    fun persistSpan(span: PersistenceSpan) {
        val spanData = mapToAvro(span)

        emitter.send(
            Message.of<SpanData?>(spanData).addMetadata(
                OutgoingKafkaRecordMetadata.builder<String?>().withKey(span.traceId).build(),
            ),
        )
    }

    private fun mapToAvro(span: PersistenceSpan): SpanData? {
        return SpanData.newBuilder()
            .setLandscapeTokenId(span.landscapeTokenId)
            .setLandscapeTokenSecret(span.landscapeTokenSecret)
            .setTraceId(span.traceId)
            .setSpanId(span.spanId)
            .setParentId(span.parentSpanId)
            .setStartTime(span.startTime)
            .setEndTime(span.endTime)
            .setApplicationName(span.applicationName)
            .setEntityDescriptor(mapSpanEntityToDescriptor(span.spanEntity)).build()
    }

    private fun mapSpanEntityToDescriptor(spanEntity: SpanEntity): Any {
        return when (spanEntity) {
            is CodeSpanEntity -> CodeDescriptor.newBuilder()
                .setFilePath(spanEntity.filePath)
                .setFunctionName(spanEntity.functionName)
                .setClassName(spanEntity.className)
                .setLanguage(spanEntity.className)
                .setCommitHash(spanEntity.gitCommitHash).build()
        }
    }
}
