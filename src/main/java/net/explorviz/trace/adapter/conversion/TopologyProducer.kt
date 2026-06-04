package net.explorviz.trace.adapter.conversion

import com.google.protobuf.InvalidProtocolBufferException
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest
import io.opentelemetry.proto.common.v1.InstrumentationScope
import io.opentelemetry.proto.resource.v1.Resource
import io.opentelemetry.proto.trace.v1.Span
import io.quarkus.logging.Log
import io.quarkus.scheduler.Scheduled
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Produces
import jakarta.inject.Inject
import java.util.concurrent.atomic.AtomicInteger
import net.explorviz.trace.adapter.service.converter.SpanConverterImpl
import net.explorviz.avro.EventType
import net.explorviz.avro.TokenEvent
import net.explorviz.trace.persistence.PersistenceSpan
import net.explorviz.trace.persistence.PersistenceSpanProcessor
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.state.KeyValueStore
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.slf4j.Logger
import org.slf4j.LoggerFactory


/** Builds a KafkaStream topology instance with all its transformers. Entry point of the stream analysis. */
@ApplicationScoped
class TopologyProducer {
    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(TopologyProducer::class.java)
    }

    private val lastReceivedSpans = AtomicInteger(0)
    private val lastInvalidSpans = AtomicInteger(0)

    @ConfigProperty(name = "explorviz.kafka-streams.topics.in") lateinit var inTopic: String

    @ConfigProperty(name = "explorviz.kafka-streams.topics.out.spans") lateinit var spansOutTopic: String

    @ConfigProperty(name = "explorviz.kafka-streams.topics.in.tokens") lateinit var tokensInTopic: String

    @ConfigProperty(name = "explorviz.kafka-streams.topics.out.tokens-table") lateinit var tokensOutTopic: String

    @Inject lateinit var spanAvroSerde: SpecificAvroSerde<net.explorviz.avro.Span>

    @Inject lateinit var tokenEventAvroSerde: SpecificAvroSerde<TokenEvent>

    @Inject lateinit var spanConverter: SpanConverterImpl

    @Inject lateinit var persistenceProcessor: PersistenceSpanProcessor

    data class SpanContext(
        val resource: Resource, val scope: InstrumentationScope, val span: Span
    )

    @Produces
    fun buildTopology(): Topology {
        val builder = StreamsBuilder()

        // BEGIN Conversion Stream
        val spanByteStream: KStream<ByteArray, ByteArray> =
            builder.stream(inTopic, Consumed.with(Serdes.ByteArray(), Serdes.ByteArray()))

        val spanStream: KStream<ByteArray, SpanContext> = spanByteStream.flatMapValues { data ->
            try {
                val spanList: List<SpanContext> =
                    ExportTraceServiceRequest.parseFrom(data).resourceSpansList.flatMap { resourceSpan ->
                        val resource: Resource = resourceSpan.resource
                        resourceSpan.scopeSpansList.flatMap { scopeSpan ->
                            val scope: InstrumentationScope = scopeSpan.scope
                            scopeSpan.spansList.map { SpanContext(resource, scope, it) }
                        }
                    }
                lastReceivedSpans.addAndGet(spanList.size)
                spanList
            } catch (e: InvalidProtocolBufferException) {
                LOGGER.trace("Invalid protocol buffer: ${e.message}")
                emptyList()
            }
        }

        val persistenceStream: KStream<ByteArray, PersistenceSpan> = spanStream.flatMapValues { spanCtx ->
            spanConverter.fromOpenTelemetrySpan(spanCtx.span, spanCtx.scope, spanCtx.resource).fold(
                onSuccess = { listOf(it) },
                onFailure = {
                    Log.tracef("Failed to convert span", it)
                    lastInvalidSpans.incrementAndGet()
                    emptyList()
                },
            )
        }

        persistenceStream.foreach { _: ByteArray?, span: PersistenceSpan? -> persistenceProcessor.accept(span) }
        // END Conversion Stream

        // BEGIN Token Stream
        builder.stream(tokensInTopic, Consumed.with(Serdes.String(), tokenEventAvroSerde)).filter { key, value ->
            LOGGER.trace("Received token event for token value {} with event {}", key, value)
            value == null || value.type == EventType.CREATED
        }.to(tokensOutTopic, Produced.with(Serdes.String(), tokenEventAvroSerde))

        builder.globalTable(
            tokensOutTopic,
            Materialized.`as`<String, TokenEvent, KeyValueStore<Bytes, ByteArray>>("token-events-global-store")
                .withKeySerde(Serdes.String()).withValueSerde(tokenEventAvroSerde),
        )
        // END Token Stream

        return builder.build()
    }

    @Scheduled(every = "{explorviz.log.span.interval}")
    fun logStatus() {
        val spans = lastReceivedSpans.getAndSet(0)
        val invalidSpans = lastInvalidSpans.getAndSet(0)
        LOGGER.debug("Received $spans spans: ${spans - invalidSpans} valid, $invalidSpans invalid")
    }
}
