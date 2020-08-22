package net.explorviz.trace.kafka;

import static net.explorviz.trace.kafka.SpanPersistingStream.SUPPRESSION_TIME_MS;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.quarkus.test.junit.QuarkusTest;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import javax.inject.Inject;
import net.explorviz.avro.SpanDynamic;
import net.explorviz.avro.Timestamp;
import net.explorviz.avro.Trace;
import net.explorviz.trace.TraceHelper;
import net.explorviz.trace.persistence.PersistingException;
import net.explorviz.trace.persistence.SpanRepository;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@QuarkusTest
class SpanPersistingStreamTest {


  private TopologyTestDriver testDriver;
  private TestInputTopic<String, SpanDynamic> inputTopic;
  private TestOutputTopic<String, Trace> outputTopic;

  private SpecificAvroSerde<SpanDynamic> spanDynamicSerde;
  private SpecificAvroSerde<Trace> traceSerDe;

  @Inject
  KafkaConfig config;

  SpanRepository mockRepo;


  @BeforeEach
  void setUp() {

    final MockSchemaRegistryClient mockSRC = new MockSchemaRegistryClient();

    assert config.getOutTopic() != null;

    mockRepo = Mockito.mock(SpanRepository.class);

    final Topology topology =
        new SpanPersistingStream(mockSRC, this.config, mockRepo).getTopology();

    this.spanDynamicSerde = new SpecificAvroSerde<>(mockSRC);


    final Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
        this.config.getTimestampExtractor());
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

    final Map<String, String> conf =
        Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://dummy");
    this.spanDynamicSerde.serializer().configure(conf, false);

    this.testDriver = new TopologyTestDriver(topology, props);

    this.inputTopic = this.testDriver.createInputTopic(this.config.getInTopic(),
        Serdes.String().serializer(), this.spanDynamicSerde.serializer());
  }

  @AfterEach
  void afterEach() {
    this.spanDynamicSerde.close();
    this.testDriver.close();
  }



  /**
   * Most basic test case.
   * Check whether two two spans with the same trace id are aggregated in the same trace.
   */
  @Test
  void testSingleSpan() throws PersistingException {

    List<Trace> mockSpanDB = new ArrayList<>();
    Mockito.doAnswer(i -> {
      Trace inserted = i.getArgumentAt(0, Trace.class);
      mockSpanDB.add(inserted);
      return null;
    }).when(mockRepo).saveTrace(Mockito.any());

    SpanDynamic testSpan = TraceHelper.randomSpan();
    inputTopic.pipeInput(testSpan.getTraceId(), testSpan);



    Assertions.assertEquals(1, mockSpanDB.size());
    Assertions.assertEquals(testSpan, mockSpanDB.get(0).getSpanList().get(0));

  }

  @Test
  void testSingleTrace() throws PersistingException {

    Map<String, Trace> mockSpanDB = new HashMap<>();
    Mockito.doAnswer(i -> {
      Trace inserted = i.getArgumentAt(0, Trace.class);
      String key = inserted.getLandscapeToken()+"::"+inserted.getTraceId();
      System.out.println("SIZE: "+inserted.getSpanList().size());
      mockSpanDB.put(key, inserted);
      return null;
    }).when(mockRepo).saveTrace(Mockito.any());

    int spansPerTrace = 20;

    Trace testTrace = TraceHelper.randomTrace(spansPerTrace);
    // Same timestamp keeps them in the same window
    Timestamp t = testTrace.getStartTime();
    for (SpanDynamic s: testTrace.getSpanList()) {
      s.setStartTime(t);
      inputTopic.pipeInput(s.getTraceId(), s);
    }
    testDriver.advanceWallClockTime(Duration.ofSeconds(2000));

    String k = testTrace.getLandscapeToken() + "::" + testTrace.getTraceId();
    Assertions.assertEquals(1, mockSpanDB.size());
    System.out.println(mockSpanDB.entrySet().toArray()[0]);
    Assertions.assertEquals(testTrace.getSpanList().size(), mockSpanDB.get(k).getSpanList().size());
  }

  @Test
  void testMultipleTraces() throws PersistingException {
    Map<String, Set<SpanDynamic>> mockSpanDB = new HashMap<>();
    Mockito.doAnswer(i -> {
      SpanDynamic inserted = i.getArgumentAt(0, SpanDynamic.class);
      String key = inserted.getLandscapeToken() + "|" + inserted.getTraceId();
      Set<SpanDynamic> spans = mockSpanDB.get(key);
      if (spans != null) {
        spans.add(inserted);
      } else {
        Set<SpanDynamic> s = new HashSet<>();
        s.add(inserted);
        mockSpanDB.put(key, s);
      }
      return null;
    }).when(mockRepo).insert(Mockito.any());

    int spansPerTrace = 20;
    int traces = 20;

    for (int i = 0; i<traces; i++) {
      Trace testTrace = TraceHelper.randomTrace(spansPerTrace);
      for (SpanDynamic s : testTrace.getSpanList()) {
        inputTopic.pipeInput(s.getTraceId(), s);
      }
    }

    Assertions.assertEquals(traces, mockSpanDB.size());
    for (Map.Entry<String, Set<SpanDynamic>> entry: mockSpanDB.entrySet()) {
      Assertions.assertEquals(spansPerTrace, entry.getValue().size());
    }

  }





}
