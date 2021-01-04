package net.explorviz.trace.kafka;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import net.explorviz.avro.SpanDynamic;
import net.explorviz.avro.Trace;
import net.explorviz.trace.persistence.PersistingException;
import net.explorviz.trace.persistence.SpanRepository;
import net.explorviz.trace.service.TraceAggregator;
import net.explorviz.trace.util.PerformanceLogger;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains a Kafka Streams topology that ingests {@link SpanDynamic} from a topic and aggregates
 * spans that belong to a the same trace in a {@link Trace} object.
 * The later contains a list of said spans as well as meta information, e.g., the landscape token.
 * The resulting traces are persisted in a cassandra db.
 */
@ApplicationScoped
public class SpanPersistingStream {

  public static final long WINDOW_SIZE_MS = 10_000;
  public static final long GRACE_MS = 2_000;

  private static final Logger LOGGER = LoggerFactory.getLogger(SpanPersistingStream.class);

  private final Properties streamsConfig = new Properties();
  private final Topology topology;

  private final SchemaRegistryClient registryClient;
  private final KafkaConfig config;

  private KafkaStreams stream;

  private SpanRepository repository;

  @Inject
  public SpanPersistingStream(final SchemaRegistryClient schemaRegistryClient,
                              final KafkaConfig config,
                              final SpanRepository repository) {

    this.registryClient = schemaRegistryClient;
    this.config = config;

    this.topology = this.buildTopology();
    this.setupStreamsConfig();

    this.repository = repository;
    this.stream = new KafkaStreams(this.topology, this.streamsConfig);

  }
  public KafkaStreams getStream() {
    return stream;
  }

  void onStart(@Observes final StartupEvent event) {

    this.stream.cleanUp();
    this.stream.start();
  }

  void onStop(@Observes final ShutdownEvent event) {
    this.stream.close();
  }

  private void setupStreamsConfig() {
    this.streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
        this.config.getBootstrapServers());
    this.streamsConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,
        this.config.getCommitIntervalMs());
    this.streamsConfig.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
        this.config.getTimestampExtractor());
    this.streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, this.config.getApplicationId());
    streamsConfig.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
        LogAndContinueExceptionHandler.class);

  }

  private Topology buildTopology() {
    PerformanceLogger pfLogger = PerformanceLogger.newOperationPerformanceLogger(
        LOGGER, 100, "Saved {} spans in {}ms");

    final StreamsBuilder builder = new StreamsBuilder();

    final KStream<String, SpanDynamic> spanStream = builder.stream(this.config.getInTopic(),
        Consumed.with(Serdes.String(), this.getAvroSerde(false)));

    TimeWindows traceWindow =
        TimeWindows.of(Duration.ofMillis(WINDOW_SIZE_MS)).grace(Duration.ofMillis(GRACE_MS));

    TraceAggregator aggregator = new TraceAggregator();

    // Group by landscapeToken::TraceId
    KTable<Windowed<String>, Trace> traceTable =
        spanStream.groupBy((k, v) -> v.getLandscapeToken() + "::" + v.getTraceId(),
            Grouped.with(Serdes.String(), getAvroSerde(false)))
            .windowedBy(traceWindow)
            .aggregate(Trace::new,
                (key, value, aggregate) -> aggregator.aggregate(aggregate, value),
                Materialized.with(Serdes.String(), this.getAvroSerde(false)))
            .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));

    KStream<String, Trace> traceStream =
        traceTable.toStream().selectKey((k, v) -> v.getLandscapeToken() + "::" + k);

    traceStream.foreach((k, t) -> {
          try {
            repository.saveTraceAsync(t);
            pfLogger.logOperation();
          } catch (PersistingException e) {
            // TODO: How to handle these spans? Enqueue somewhere for retries?
            if (LOGGER.isErrorEnabled()) {
              LOGGER.error("A span was not persisted: {0}", e);
            }
          }
        });

    return builder.build();
  }

  /**
   * Creates a {@link Serde} for specific avro records using the {@link SpecificAvroSerde}
   *
   * @param forKey {@code true} if the Serde is for keys, {@code false} otherwise
   * @param <T>    type of the avro record
   * @return a Serde
   */
  private <T extends SpecificRecord> SpecificAvroSerde<T> getAvroSerde(final boolean forKey) {
    final SpecificAvroSerde<T> serde = new SpecificAvroSerde<>(this.registryClient);
    serde.configure(Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
        this.config.getSchemaRegistryUrl()), forKey);

    return serde;
  }



  public Topology getTopology() {
    return this.topology;
  }


}

