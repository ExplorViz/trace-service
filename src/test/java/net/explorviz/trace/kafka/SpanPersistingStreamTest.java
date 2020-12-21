package net.explorviz.trace.kafka;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.quarkus.test.junit.QuarkusMock;
import io.quarkus.test.junit.QuarkusTest;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.inject.Inject;
import net.explorviz.avro.SpanDynamic;
import net.explorviz.avro.Timestamp;
import net.explorviz.avro.Trace;
import net.explorviz.trace.TraceHelper;
import net.explorviz.trace.events.TokenEventConsumer;
import net.explorviz.trace.persistence.PersistingException;
import net.explorviz.trace.persistence.SpanRepository;
import net.explorviz.trace.service.TimestampHelper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;


class SpanPersistingStreamTest {


  private TopologyTestDriver testDriver;
  private TestInputTopic<String, SpanDynamic> inputTopic;
  private SpecificAvroSerde<SpanDynamic> spanDynamicSerde;

  SpanRepository mockRepo;


  @BeforeEach
  void setUp() {

    final MockSchemaRegistryClient mockSRC = new MockSchemaRegistryClient();

    mockRepo = Mockito.mock(SpanRepository.class);
    KafkaConfig config = Utils.testKafkaConfigs();

    final Topology topology = new SpanPersistingStream(mockSRC, config, mockRepo).getTopology();

    this.spanDynamicSerde = new SpecificAvroSerde<>(mockSRC);


    final Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
        config.getTimestampExtractor());
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

    final Map<String, String> conf =
        Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://dummy");
    this.spanDynamicSerde.serializer().configure(conf, false);

    this.testDriver = new TopologyTestDriver(topology, props);

    this.inputTopic = this.testDriver.createInputTopic(config.getInTopic(),
        Serdes.String().serializer(), this.spanDynamicSerde.serializer());
  }

  @AfterEach
  void afterEach() {
    this.spanDynamicSerde.close();
    testDriver.getAllStateStores().forEach((k,v) -> v.close());
    this.testDriver.close();
  }



  /**
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
    forceSuppression(testSpan.getStartTime());

    Assertions.assertEquals(1, mockSpanDB.size());
    Assertions.assertEquals(testSpan, mockSpanDB.get(0).getSpanList().get(0));

  }

  @Test
  void testSingleTrace() throws PersistingException {

    Map<String, Trace> mockSpanDB = new HashMap<>();
    Mockito.doAnswer(i -> {
      Trace inserted = i.getArgumentAt(0, Trace.class);
      String key = inserted.getLandscapeToken() + "::" + inserted.getTraceId();
      mockSpanDB.put(key, inserted);
      return null;
    }).when(mockRepo).saveTrace(Mockito.any());

    int spansPerTrace = 20;

    Trace testTrace = TraceHelper.randomTrace(spansPerTrace);
    Timestamp t = testTrace.getStartTime();
    for (SpanDynamic s : testTrace.getSpanList()) {
      t = Timestamp.newBuilder(t).setNanoAdjust(t.getNanoAdjust() + 1).build();
      s.setStartTime(t);
      inputTopic.pipeInput(s.getTraceId(), s);
    }
    forceSuppression(t);

    String k = testTrace.getLandscapeToken() + "::" + testTrace.getTraceId();
    Assertions.assertEquals(1, mockSpanDB.size());
    Assertions.assertEquals(testTrace.getSpanList().size(), mockSpanDB.get(k).getSpanList().size());
  }

  @Test
  void testMultipleTraces() throws PersistingException {
    Map<String, Trace> mockSpanDB = new HashMap<>();
    Mockito.doAnswer(i -> {
      Trace inserted = i.getArgumentAt(0, Trace.class);
      String key = inserted.getLandscapeToken() + "::" + inserted.getTraceId();
      mockSpanDB.put(key, inserted);
      return null;
    }).when(mockRepo).saveTrace(Mockito.any());

    int spansPerTrace = 20;
    int traceAmount = 20;

    // Create multiple traces that happen in parallel
    List<KeyValue<String, SpanDynamic>> traces = new ArrayList<>();
    Timestamp baseTime = TraceHelper.randomTrace(1).getStartTime();
    for (int i = 0; i < traceAmount; i++) {
      Trace testTrace = TraceHelper.randomTrace(spansPerTrace);
      Timestamp t = Timestamp.newBuilder(baseTime).build();
      for (SpanDynamic s : testTrace.getSpanList()) {
        // Keep spans in one window
        t = Timestamp.newBuilder(t).setNanoAdjust(t.getNanoAdjust() + 2).build();
        s.setStartTime(t);
        traces.add(new KeyValue<>(s.getTraceId(), s));
      }
    }

    inputTopic.pipeKeyValueList(traces);
    forceSuppression(baseTime);

    Assertions.assertEquals(traceAmount, mockSpanDB.size());
    for (Map.Entry<String, Trace> entry : mockSpanDB.entrySet()) {
      Assertions.assertEquals(spansPerTrace, entry.getValue().getSpanList().size());
    }

  }


  @Test
  void testOutOfWindow() {
    Map<String, Trace> mockSpanDB = new HashMap<>();
    Mockito.doAnswer(i -> {
      Trace inserted = i.getArgumentAt(0, Trace.class);
      String key = inserted.getLandscapeToken() + "::" + inserted.getTraceId();
      mockSpanDB.computeIfPresent(key, (k, v) -> {
        v.getSpanList().addAll(inserted.getSpanList());
        return v;
      });
      mockSpanDB.putIfAbsent(key, inserted);
      return null;
    }).when(mockRepo).saveTrace(Mockito.any());

    int spansPerTrace = 20;

    Trace testTrace = TraceHelper.randomTrace(spansPerTrace);
    Timestamp ts = TraceHelper.randomTrace(1).getStartTime();
    for (int i = 0; i < testTrace.getSpanList().size(); i++) {
      SpanDynamic s = testTrace.getSpanList().get(i);
      if (i < testTrace.getSpanList().size() - 1) {
        ts = Timestamp.newBuilder(ts).setNanoAdjust(ts.getNanoAdjust() + 1).build();
      } else {
        // Last span arrives out of window
        long secs = Duration.ofMillis(SpanPersistingStream.WINDOW_SIZE_MS).toSeconds();
        ts = Timestamp.newBuilder(ts)
            .setSeconds(ts.getSeconds() + secs).build();
      }
      s.setStartTime(ts);
      inputTopic.pipeInput(s.getTraceId(), s);
    }

    forceSuppression(ts);

    String k = testTrace.getLandscapeToken() + "::" + testTrace.getTraceId();
    Assertions.assertEquals(1, mockSpanDB.size());
    Assertions.assertEquals(testTrace.getSpanList().size(), mockSpanDB.get(k).getSpanList().size());
  }



  /**
   * Forces the suppression to emit results by sending a dummy event with a timestamp
   * larger than the suppression time.
   */
  private void forceSuppression(Timestamp lastTimestamp) {
    Duration secs = Duration.ofMillis(SpanPersistingStream.WINDOW_SIZE_MS)
        .plusMillis(SpanPersistingStream.GRACE_MS);
    SpanDynamic dummy = TraceHelper.randomSpan();

    Instant ts = TimestampHelper.toInstant(lastTimestamp).plus(secs);
    dummy.setStartTime(TimestampHelper.toTimestamp(ts));
    inputTopic.pipeInput(dummy.getTraceId(), dummy);
  }



}
