package net.explorviz.trace.kafka;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.quarkus.test.junit.QuarkusTest;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import javax.inject.Inject;
import net.explorviz.avro.SpanDynamic;
import net.explorviz.avro.Timestamp;
import net.explorviz.avro.Trace;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
class TraceAggregationStreamTest {


  private TopologyTestDriver testDriver;
  private TestInputTopic<String, SpanDynamic> inputTopic;
  private TestOutputTopic<String, Trace> outputTopic;

  private SpecificAvroSerde<SpanDynamic> evSpanSerDe;
  private SpecificAvroSerde<Trace> traceSerDe;

  @Inject
  KafkaConfig config;


  @BeforeEach
  void setUp() {

    final MockSchemaRegistryClient mockSRC = new MockSchemaRegistryClient();

    assert config.getOutTopic() != null;

    final Topology topology =
        new TraceAggregationStream(mockSRC, this.config).getTopology();

    this.evSpanSerDe = new SpecificAvroSerde<>(mockSRC);
    this.traceSerDe = new SpecificAvroSerde<>(mockSRC);

    final Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
        this.config.getTimestampExtractor());
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

    final Map<String, String> conf =
        Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://dummy");
    this.evSpanSerDe.serializer().configure(conf, false);

    this.traceSerDe.deserializer().configure(conf, false);

    this.testDriver = new TopologyTestDriver(topology, props);

    this.inputTopic = this.testDriver.createInputTopic(this.config.getInTopic(),
        Serdes.String().serializer(), this.evSpanSerDe.serializer());
    this.outputTopic = this.testDriver.createOutputTopic(this.config.getOutTopic(),
        Serdes.String().deserializer(), this.traceSerDe.deserializer());
  }

  @AfterEach
  void afterEach() {
    this.evSpanSerDe.close();
    this.traceSerDe.close();
    this.testDriver.close();
  }



  /**
   * Most basic test case.
   * Check whether two two spans with the same trace id are aggregated in the same trace.
   */
  @Test
  void testAggregation() {


    Instant start = Instant.now().minusSeconds(5);
    Instant end = Instant.now();
    String tid = "trace1";
    String token = "tok";
    SpanDynamic s1 = SpanDynamic.newBuilder()
        .setSpanId("span1")
        .setTraceId(tid)
        .setStartTime(new Timestamp(start.getEpochSecond(), start.getNano()))
        .setEndTime(new Timestamp(end.getEpochSecond(), end.getNano()))
        .setHashCode("hash")
        .setParentSpanId(token)
        .setLandscapeToken("adasod")
        .build();
    SpanDynamic s2 = SpanDynamic.newBuilder(s1)
        .setSpanId("span2").setParentSpanId("span1").build();

    inputTopic.pipeInput(tid, s1);
    inputTopic.pipeInput(tid, s2);

    Map<String, Trace> resultTable = outputTopic.readKeyValuesToMap();
    System.out.println(resultTable);

    ReadOnlyKeyValueStore<String, Trace> traceStore = testDriver.getKeyValueStore(KafkaConfig.TRACES_STORE);
    Trace aggregated = traceStore.get(tid);
    Assertions.assertTrue(aggregated.getSpanList().contains(s1));
    Assertions.assertTrue(aggregated.getSpanList().contains(s2));
  }

  @Test
  void testTraceTimes() {

    Instant start = Instant.now();
    Instant end = Instant.now();
    String tid = "trace1";
    String token = "tok";
    SpanDynamic s1 = SpanDynamic.newBuilder()
        .setSpanId("span1")
        .setTraceId(tid)
        .setStartTime(new Timestamp(start.minusSeconds(2).getEpochSecond(), start.getNano()))
        .setEndTime(new Timestamp(end.getEpochSecond(), end.getNano()))
        .setHashCode("hash")
        .setLandscapeToken(token)
        .build();
    // Started After s1
    SpanDynamic s2 = SpanDynamic.newBuilder(s1)
        .setStartTime(new Timestamp(start.minusSeconds(5).getEpochSecond(), start.getNano()))
        .setEndTime(new Timestamp(end.getEpochSecond(), end.getNano()))
        .setSpanId("span2").setParentSpanId("span1").build();


    inputTopic.pipeInput(tid, s1);
    inputTopic.pipeInput(tid, s2);

    Map<String, Trace> resultTable = outputTopic.readKeyValuesToMap();
    System.out.println(resultTable);

    ReadOnlyKeyValueStore<String, Trace> traceStore = testDriver.getKeyValueStore(KafkaConfig.TRACES_STORE);
    Trace aggregated = traceStore.get(tid);
    Assertions.assertEquals(s2.getStartTime(), aggregated.getStartTime());
    Assertions.assertEquals(s1.getEndTime(), aggregated.getEndTime());

  }

  @Test
  void testChangelog() {
    Instant start = Instant.now();
    Instant end = Instant.now();
    String tid = "trace1";
    String token = "tok";
    SpanDynamic s1 = SpanDynamic.newBuilder()
        .setSpanId("span1")
        .setTraceId(tid)
        .setStartTime(new Timestamp(start.minusSeconds(2).getEpochSecond(), start.getNano()))
        .setEndTime(new Timestamp(end.getEpochSecond(), end.getNano()))
        .setHashCode("hash")
        .setLandscapeToken(token)
        .build();
    // Started After s1
    SpanDynamic s2 = SpanDynamic.newBuilder(s1)
        .setStartTime(new Timestamp(start.minusSeconds(5).getEpochSecond(), start.getNano()))
        .setEndTime(new Timestamp(end.getEpochSecond(), end.getNano()))
        .setSpanId("span2").setParentSpanId("span1").build();

    // Pipe in wrong sorting order
    inputTopic.pipeInput(tid, s1);
    inputTopic.pipeInput(tid, s2);

    KeyValue<String, Trace> firstChange = outputTopic.readKeyValue();
    KeyValue<String, Trace> secondChange = outputTopic.readKeyValue();


  }


}
