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
import net.explorviz.avro.EVSpan;
import net.explorviz.avro.EVSpanData;
import net.explorviz.avro.EVSpanKey;
import net.explorviz.avro.Timestamp;
import net.explorviz.avro.Trace;
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

    final KStream<String, EVSpan> explSpanStream = builder.stream(this.config.getInTopic(),
        Consumed.with(Serdes.String(), this.getAvroSerde(false)));

    // Window spans in 4s intervals with 2s grace period
    final TimeWindowedKStream<String, EVSpan> windowedEvStream =
        explSpanStream.groupByKey().windowedBy(TimeWindows.of(WINDOW_SIZE).grace(GRACE_PERIOD));

    // Aggregate Spans to traces and deduplicate similar spans of a trace
    final KTable<Windowed<String>, Trace> traceTable =
        windowedEvStream.aggregate(Trace::new, (traceId, evSpan, trace) -> {

          // Initialize Span according to first span of the trace
          final long evSpanEndTime = evSpan.getEndTime();
          if (trace.getSpanList() == null) {
            trace.setSpanList(new ArrayList<>());
            trace.getSpanList().add(evSpan);

            trace.setStartTime(evSpan.getStartTime());
            trace.setEndTime(evSpanEndTime);
            trace.setOverallRequestCount(1);
            trace.setDuration(Duration.between(this.tsToInstant(trace.getStartTime()),
                Instant.ofEpochMilli(evSpanEndTime)).toNanos());

            trace.setTraceCount(1);

            // set initial trace id - do not change, since this is the major key for kafka
            // partitioning
            trace.setTraceId(evSpan.getTraceId());
          } else {

            // TODO
            // Implement
            // - traceDuration
            // - Tracesteps with caller callee each = EVSpan


            // Find duplicates in Trace (via fqn), aggregate based on request count
            // Furthermore, potentially update trace values
            trace.getSpanList().stream()
                .filter(s -> s.getOperationName().contentEquals(evSpan.getOperationName()))
                .findAny().ifPresentOrElse(s -> {
              s.setRequestCount(s.getRequestCount() + 1);

              if (this.tsToInstant(evSpan.getStartTime())
                  .isBefore(this.tsToInstant(s.getStartTime()))) {
                s.setStartTime(evSpan.getStartTime());
              }

              s.setEndTime(Math.max(s.getEndTime(), evSpan.getEndTime()));
            }, () -> trace.getSpanList().add(evSpan));
            trace.getSpanList().stream().map(s -> this.tsToInstant(s.getStartTime()))
                .min(Instant::compareTo)
                .ifPresent(s -> trace.setStartTime(new Timestamp(s.getEpochSecond(), s.getNano())));
            trace.getSpanList().stream().mapToLong(EVSpan::getEndTime).max()
                .ifPresent(trace::setEndTime);
            final long duration = Duration.between(this.tsToInstant(trace.getStartTime()),
                Instant.ofEpochMilli(trace.getEndTime())).toNanos();
            trace.setDuration(duration);


          }
          return trace;
        }, Materialized.with(Serdes.String(), this.getAvroSerde(false)));

    final KStream<Windowed<String>, Trace> traceStream = traceTable.toStream();


    // Map traces to a new key that resembles all included spans
    final KStream<Windowed<EVSpanKey>, Trace> traceIdSpanStream =
        traceStream.flatMap((key, trace) -> {

          final List<KeyValue<Windowed<EVSpanKey>, Trace>> result = new LinkedList<>();

          final List<EVSpanData> spanDataList = new ArrayList<>();

          for (final EVSpan span : trace.getSpanList()) {
            spanDataList.add(
                new EVSpanData(span.getOperationName(), span.getHostname(), span.getAppName()));
          }

          final EVSpanKey newKey = new EVSpanKey(spanDataList);

          final Windowed<EVSpanKey> newWindowedKey = new Windowed<>(newKey, key.window());

          result.add(KeyValue.pair(newWindowedKey, trace));
          return result;
        });


    // Reduce similar Traces of one window to a single Trace
    final KTable<Windowed<EVSpanKey>, Trace> reducedTraceTable = traceIdSpanStream
        .groupByKey(Grouped.with(this.getWindowedAvroSerde(WINDOW_SIZE), this.getAvroSerde(false)))
        .aggregate(Trace::new, (sharedTraceKey, trace, reducedTrace) -> {


          if (reducedTrace.getTraceId() == null) {
            reducedTrace = trace;
          } else {
            reducedTrace.setTraceCount(reducedTrace.getTraceCount() + 1);
            // Use the Span list of the latest trace in the group
            // Do so since span list only grow but never loose elements
            reducedTrace.setSpanList(trace.getSpanList());

            // Update start and end time of the trace

            if (this.tsToInstant(trace.getStartTime())
                .isBefore(this.tsToInstant(reducedTrace.getStartTime()))) {
              reducedTrace.setStartTime(trace.getStartTime());
            }


            reducedTrace.setEndTime(Math.max(trace.getEndTime(), reducedTrace.getEndTime()));
          }

          return reducedTrace;
        }, Materialized.with(this.getWindowedAvroSerde(WINDOW_SIZE), this.getAvroSerde(false)));
    // .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));

    final KStream<Windowed<EVSpanKey>, Trace> reducedTraceStream = reducedTraceTable.toStream();

    final KStream<String, Trace> reducedIdTraceStream = reducedTraceStream.flatMap((key, value) -> {

      final List<KeyValue<String, Trace>> result = new LinkedList<>();

      result.add(KeyValue.pair(value.getTraceId(), value));
      return result;
    });

    reducedIdTraceStream.foreach((key, trace) -> {

      final List<EVSpan> list = trace.getSpanList();
      System.out.println("Trace with id " + trace.getTraceId());
      list.forEach((val) -> {
        System.out
            .println(this.tsToInstant(val.getStartTime()).toEpochMilli() + " : " + val.getEndTime()
                + " für " + val.getOperationName() + " mit Anzahl " + val.getRequestCount());
      });

    });

    // Sort spans in each trace based of start time
    reducedIdTraceStream.peek((key, trace) -> trace.getSpanList().sort((s1, s2) -> {
      final Instant instant1 = this.tsToInstant(s1.getStartTime());
      final Instant instant2 = this.tsToInstant(s2.getStartTime());
      return instant1.compareTo(instant2);
    }));

    // TODO implement count attribute in Trace -> number of similar traces
    // TODO Reduce traceIdAndAllTracesStream to similiar traces stream (map and reduce)
    // use something like hash for trace
    // https://docs.confluent.io/current/streams/quickstart.html#purpose

    reducedIdTraceStream.to(this.config.getOutTopic(),
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

