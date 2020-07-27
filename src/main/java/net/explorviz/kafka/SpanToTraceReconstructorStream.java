package net.explorviz.kafka;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import net.explorviz.avro.SpanDynamic;
import net.explorviz.avro.EVSpanData;
import net.explorviz.avro.EVSpanKey;
import net.explorviz.avro.Timestamp;
import net.explorviz.avro.Trace;
import net.explorviz.service.SpanToTraceAggregator;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;

/**
 * Collects spans for 10 seconds, grouped by the trace id, and forwards the resulting batch to the
 * topic 'span-batches'
 */
@ApplicationScoped
public class SpanToTraceReconstructorStream {

  private static final Duration WINDOW_SIZE = Duration.ofSeconds(4);
  private static final Duration GRACE_PERIOD = Duration.ofSeconds(2);

  private final Properties streamsConfig = new Properties();

  private final Topology topology;

  private final SchemaRegistryClient registryClient;
  private final KafkaConfig config;

  private KafkaStreams streams;

  @Inject
  public SpanToTraceReconstructorStream(final SchemaRegistryClient schemaRegistryClient,
                                        final KafkaConfig config) {

    this.registryClient = schemaRegistryClient;
    this.config = config;

    this.topology = this.buildTopology();
    this.setupStreamsConfig();
  }

  void onStart(@Observes final StartupEvent event) {
    this.streams = new KafkaStreams(this.topology, this.streamsConfig);
    this.streams.cleanUp();
    this.streams.start();
  }

  void onStop(@Observes final ShutdownEvent event) {
    this.streams.close();
  }

  private void setupStreamsConfig() {
    this.streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
        this.config.getBootstrapServers());
    this.streamsConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,
        this.config.getCommitIntervalMs());
    this.streamsConfig.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
        this.config.getTimestampExtractor());
    this.streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, this.config.getApplicationId());
  }

  private Topology buildTopology() {
    final StreamsBuilder builder = new StreamsBuilder();

    final KStream<String, SpanDynamic> explSpanStream = builder.stream(this.config.getInTopic(),
        Consumed.with(Serdes.String(), this.getAvroSerde(false)));

    /*
     * Wenn Landscape Token nicht im Key: Traces verschiedenen Landscapes mit gleichen IDs werden
     * vermischt!
     */

    /*
     * Windowed aggregation wirklich notwendig?
     * Traces sind eindeutig durch Trace-ID bestimmt.
     * Aggregation in KTable: Neue Span -> Trace update -> Neuer Trace auf KStream für TraceId
     * Jedes Update kann in Datenbank zu Update an Trace führen.
     *  -> Statt Datenbank: Kafka Selbst (Stores?)
     *        - LandscapeToken muss dann in Key (MUSS SOWIESO!)
     *        - Löst auch das "Wie serialisieren"-Problem
     *
     * GGf. Nachteil:  Abfrage nach Spans für TraceID liefert zu zwei verschiedenen Zeitpunkten
     * verschiedene Ergebnisse, d.h. bei zu frühen Anfragen ist Trace noch unvollständig
     *
     * Aktuell: Span kommt zu spät -> Neues Window für gleichen Trace
     *
     * Testcase:  "Spans with the same trace id in close temporal proximity should
     * be aggregated in the same trace. If another span with the same trace id arrives later, it
     * should not be included in the same trace."
     *  -> Warum nicht? Gehört doch in diesen
     *
     */


    // Window spans in 4s intervals with 2s grace period
    final TimeWindowedKStream<String, SpanDynamic> windowedEvStream =
        explSpanStream.groupByKey().windowedBy(TimeWindows.of(WINDOW_SIZE).grace(GRACE_PERIOD));

    // Aggregate Spans to traces and deduplicate similar spans of a trace
    SpanToTraceAggregator aggregator = new SpanToTraceAggregator();
    final KTable<Windowed<String>, Trace> traceTable =
        windowedEvStream.aggregate(Trace::new,
            (traceId, span, trace) -> aggregator.aggregate(traceId, trace, span),
            Materialized.with(Serdes.String(), this.getAvroSerde(false)))
            .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));

    // Add traces to stream independent of their windows
    final KStream<String, Trace> traceStream =
        traceTable.toStream().map((key, value) -> new KeyValue<>(key.key(), value));



    traceStream.to(this.config.getOutTopic(),
        Produced.with(Serdes.String(), this.getAvroSerde(false)));
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

  /**
   * Creates a new Serde for windowed keys of specific avro records
   *
   * @param <T> avro record data type
   * @return a {@link Serde} for specific avro records wrapped in a time window
   */
  private <T extends SpecificRecord> Serde<Windowed<T>> getWindowedAvroSerde(
      final Duration windowSizeInMs) {

    final Serde<T> keySerde = this.getAvroSerde(true);

    return new WindowedSerdes.TimeWindowedSerde<>(keySerde, windowSizeInMs.toMillis());
  }

  private Instant tsToInstant(final Timestamp ts) {
    return Instant.ofEpochSecond(ts.getSeconds(), ts.getNanoAdjust());
  }

  public Topology getTopology() {
    return this.topology;
  }


}

