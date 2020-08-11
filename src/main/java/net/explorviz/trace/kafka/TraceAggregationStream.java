package net.explorviz.kafka;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import java.util.Map;
import java.util.Properties;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import net.explorviz.avro.SpanDynamic;
import net.explorviz.avro.Trace;
import net.explorviz.service.TraceAggregator;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Contains a Kafka Streams topology that ingests {@link SpanDynamic} from a topic and aggregates
 * spans that belong to a the same trace in a {@link Trace} object.
 * The later contains a list of said spans as well as meta information, e.g., the landscape token.
 * The resulting traces are persisted in a cassandra db.
 */
@ApplicationScoped
public class TraceAggregationStream {

  private final Properties streamsConfig = new Properties();

  private final Topology topology;

  private final SchemaRegistryClient registryClient;
  private final KafkaConfig config;
  private final Materialized<String, Trace, KeyValueStore<Bytes, byte[]>> traceStore;

  private KafkaStreams streams;

  @Inject
  public TraceAggregationStream(final SchemaRegistryClient schemaRegistryClient,
                                final KafkaConfig config) {

    this.registryClient = schemaRegistryClient;
    this.config = config;

    assert config.getOutTopic() != null;
    this.traceStore = createTraceStore();
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

    final KStream<String, SpanDynamic> spanStream = builder.stream(this.config.getInTopic(),
        Consumed.with(Serdes.String(), this.getAvroSerde(false)));

    // Aggregate Spans to traces and store them in a state store "traces"
    TraceAggregator aggregator = new TraceAggregator();
    final KTable<String, Trace> traceTable =
        spanStream.groupByKey().aggregate(Trace::new,
            (key, value, aggregate) -> aggregator.aggregate(key, aggregate, value),
            this.traceStore);

    // Stream the changelog to index, remove span list to avoid unnecessary data transfer
    traceTable
        .toStream()
        .to(this.config.getOutTopic(),
            Produced.with(Serdes.String(), this.getAvroSerde(false)));



    return builder.build();
  }

  private Materialized<String, Trace, KeyValueStore<Bytes, byte[]>> createTraceStore() {
    return Materialized.<String, Trace, KeyValueStore<Bytes, byte[]>>as(KafkaConfig.TRACES_STORE)
        .withKeySerde(Serdes.String()).withValueSerde(this.getAvroSerde(false));
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

