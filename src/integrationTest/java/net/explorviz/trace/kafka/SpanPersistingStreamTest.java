package net.explorviz.trace.kafka;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import net.explorviz.avro.SpanDynamic;
import net.explorviz.avro.Timestamp;
import net.explorviz.avro.Trace;
import net.explorviz.trace.TraceHelper;
import net.explorviz.trace.service.TimestampHelper;
import net.explorviz.trace.service.TraceRepository;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

class SpanPersistingStreamTest {

  private TopologyTestDriver testDriver;
  private TestInputTopic<String, SpanDynamic> inputTopic;
  private SpecificAvroSerde<SpanDynamic> spanDynamicSerde;

  private TraceRepository traceRepository;

  @BeforeEach
  void setUp() {

    final MockSchemaRegistryClient mockSRC = new MockSchemaRegistryClient();

    this.traceRepository = Mockito.mock(TraceRepository.class);

    final KafkaConfig config = Utils.testKafkaConfigs();

    final Topology topology =
        new SpanPersistingStream(mockSRC, config, this.traceRepository).getTopology();

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
    this.testDriver.getAllStateStores().forEach((k, v) -> v.close());
    this.testDriver.close();
  }

  /**
   * Check if a single span has been saved in a trace.
   */
  @Test
  void testSingleSpan() {

    final List<Trace> mockSpanDB = new ArrayList<>();

    Mockito.doAnswer(i -> {
      final Trace inserted = i.getArgument(0, Trace.class);
      mockSpanDB.add(inserted);
      return null;
    }).when(this.traceRepository).insert(ArgumentMatchers.any(Trace.class));

    final SpanDynamic testSpan = TraceHelper.randomSpan();

    this.inputTopic.pipeInput(testSpan.getTraceId(), testSpan);
    this.forceSuppression(testSpan.getStartTime());

    Assertions.assertEquals(1, mockSpanDB.size());
    Assertions.assertEquals(testSpan, mockSpanDB.get(0).getSpanList().get(0));
  }

  @Test
  void testSingleTrace() {

    final Map<String, Trace> mockSpanDB = new HashMap<>();
    Mockito.doAnswer(i -> {
      final Trace inserted = i.getArgument(0, Trace.class);
      final String key = inserted.getLandscapeToken() +
          "::" + inserted.getTraceId();
      mockSpanDB.put(key, inserted);
      return null;
    }).when(this.traceRepository).insert(ArgumentMatchers.any(Trace.class));

    final int spansPerTrace = 20;

    final Trace testTrace = TraceHelper.randomTrace(spansPerTrace);
    Timestamp t =
        testTrace.getStartTime();
    for (final SpanDynamic s : testTrace.getSpanList()) {
      t =
          Timestamp.newBuilder(t).setNanoAdjust(t.getNanoAdjust() + 1).build();
      s.setStartTime(t);
      this.inputTopic.pipeInput(s.getTraceId(), s);
    }
    this.forceSuppression(t);

    final String k = testTrace.getLandscapeToken() + "::" + testTrace.getTraceId();
    Assertions.assertEquals(1, mockSpanDB.size());
    Assertions.assertEquals(testTrace.getSpanList().size(),
        mockSpanDB.get(k).getSpanList().size());
  }

  @Test
  void testMultipleTraces() {
    final Map<String, Trace> mockSpanDB = new HashMap<>();

    Mockito.doAnswer(i -> {
      final Trace inserted = i.getArgument(0, Trace.class);
      final String key = inserted.getLandscapeToken() + "::" + inserted.getTraceId();
      mockSpanDB.put(key, inserted);
      return null;
    }).when(this.traceRepository).insert(ArgumentMatchers.any(Trace.class));

    final int spansPerTrace = 20;
    final int traceAmount = 20;

    // Create multiple traces that happen in parallel
    final List<KeyValue<String, SpanDynamic>> traces = new ArrayList<>();
    final Timestamp baseTime =
        TraceHelper.randomTrace(1).getStartTime();
    for (int i = 0; i < traceAmount; i++) {
      final Trace testTrace = TraceHelper.randomTrace(spansPerTrace);
      Timestamp t =
          Timestamp.newBuilder(baseTime).build();
      for (final SpanDynamic s : testTrace.getSpanList()) {
        // Keep spans in one window
        t = Timestamp.newBuilder(t).setNanoAdjust(t.getNanoAdjust() +
            2).build();
        s.setStartTime(t);
        traces.add(new KeyValue<>(s.getTraceId(), s));
      }
    }

    this.inputTopic.pipeKeyValueList(traces);
    this.forceSuppression(baseTime);

    for (final Map.Entry<String, Trace> entry : mockSpanDB.entrySet()) {
      Assertions.assertEquals(spansPerTrace,
          entry.getValue().getSpanList().size());
    }

  }


  @Test
  @Disabled
  void testOutOfWindow() {

    // callback to get traces after analysis
    final Map<String, Trace> mockSpanDB = new HashMap<>();
    Mockito.doAnswer(i -> {
      final Trace inserted = i.getArgument(0, Trace.class);
      final String key = inserted.getLandscapeToken() + "::" + inserted.getTraceId();
      mockSpanDB.computeIfPresent(key, (k, v) -> {
        v.getSpanList().addAll(inserted.getSpanList());
        return v;
      });
      mockSpanDB.putIfAbsent(key, inserted);
      return null;
    }).when(this.traceRepository).insert(ArgumentMatchers.any(Trace.class));


    // push spans on topic
    final int spansPerTrace = 20;

    final Trace testTrace = TraceHelper.randomTrace(spansPerTrace);
    Timestamp ts = TraceHelper.randomTrace(1).getStartTime();
    for (int i = 0; i < testTrace.getSpanList().size(); i++) {
      final SpanDynamic s = testTrace.getSpanList().get(i);
      if (i < testTrace.getSpanList().size() - 1) {
        ts = Timestamp.newBuilder(ts).setNanoAdjust(ts.getNanoAdjust() + 1).build();
      } else {
        // Last span final arrives out final of window
        final long secs = Duration.ofMillis(SpanPersistingStream.WINDOW_SIZE_MS).toSeconds();
        ts =
            Timestamp.newBuilder(ts).setSeconds(ts.getSeconds() + secs).build();
      }
      s.setStartTime(ts);
      this.inputTopic.pipeInput(s.getTraceId(), s);
    }

    this.forceSuppression(ts);

    final String k = testTrace.getLandscapeToken() + "::" + testTrace.getTraceId();
    Assertions.assertEquals(1, mockSpanDB.size());
    Assertions.assertEquals(testTrace.getSpanList().size(),
        mockSpanDB.get(k).getSpanList().size());
  }


  /**
   * Forces the suppression to emit results by sending a dummy event with a timestamp larger than
   * the suppression time.
   */
  private void forceSuppression(final Timestamp lastTimestamp) {
    final Duration secs = Duration.ofMillis(SpanPersistingStream.WINDOW_SIZE_MS)
        .plusMillis(SpanPersistingStream.GRACE_MS);
    final SpanDynamic dummy = TraceHelper.randomSpan();

    final Instant ts = TimestampHelper.toInstant(lastTimestamp).plus(secs);
    dummy.setStartTime(TimestampHelper.toTimestamp(ts));
    this.inputTopic.pipeInput(dummy.getTraceId(), dummy);
  }



}
