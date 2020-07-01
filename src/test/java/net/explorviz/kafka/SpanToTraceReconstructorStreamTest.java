package net.explorviz.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.quarkus.test.junit.QuarkusTest;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.inject.Inject;
import net.explorviz.avro.EVSpan;
import net.explorviz.avro.Timestamp;
import net.explorviz.avro.Trace;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
class SpanToTraceReconstructorStreamTest {


  private TopologyTestDriver testDriver;
  private TestInputTopic<String, EVSpan> inputTopic;
  private TestOutputTopic<String, Trace> outputTopic;

  private SpecificAvroSerde<EVSpan> evSpanSerDe;
  private SpecificAvroSerde<Trace> traceSerDe;

  @Inject
  KafkaConfig config;

  @BeforeEach
  void setUp() throws IOException, RestClientException {

    final MockSchemaRegistryClient mockSRC = new MockSchemaRegistryClient();

    final Topology topology =
        new SpanToTraceReconstructorStream(mockSRC, this.config).getTopology();

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
   * Tests whether multiple spans with the same operation name belonging to the same trace are
   * reduced to a single span with an updated {@link EVSpan#requestCount}
   */
  @Test
  void testSpanDeduplication() {

    final String traceId = "testtraceid";
    final String operationName = "OpName";

    final Timestamp start1 = new Timestamp(10L, 0);
    final long end1 = 20L;


    final Timestamp start2 = new Timestamp(10L, 2323);
    final long end2 = 80L;


    final EVSpan evSpan1 = EVSpan.newBuilder()
        .setLandscapeToken("tok")
        .setSpanId("1")
        .setTraceId(traceId)
        .setStartTime(start1)
        .setEndTime(end1)
        .setDuration(10L)
        .setOperationName(operationName)
        .setRequestCount(1)
        .setHostname("sampleHost")
        .setHostIpAddress("1.2.3.4")
        .setAppName("sampleapp")
        .setAppPid("1234")
        .setAppLanguage("lang")
        .build();
    final EVSpan evSpan2 = EVSpan.newBuilder()
        .setLandscapeToken("tok")
        .setSpanId("2")
        .setTraceId(traceId)
        .setStartTime(start2)
        .setEndTime(end2)
        .setDuration(40L)
        .setOperationName(operationName)
        .setRequestCount(1)
        .setHostname("sampleHost")
        .setHostIpAddress("1.2.3.4")
        .setAppName("sampleapp")
        .setAppPid("1234")
        .setAppLanguage("lang")
        .build();

    this.inputTopic.pipeInput(evSpan1.getTraceId(), evSpan1);
    this.inputTopic.pipeInput(evSpan2.getTraceId(), evSpan2);


    final List<KeyValue<String, Trace>> records = this.outputTopic.readKeyValuesToList();

    assertEquals(2, records.size());

    assertEquals(traceId, records.get(0).key);
    assertEquals(traceId, records.get(1).key);

    // Trace is "completed" after two updates, thus take the second record
    final Trace trace = records.get(1).value;

    assertEquals(start1, trace.getStartTime());
    assertEquals(end2, trace.getEndTime());

    // Deduplication
    assertEquals(1, trace.getSpanList().size());
    assertEquals(2, trace.getSpanList().get(0).getRequestCount());

  }


  /**
   * Tests if a trace's span list is sorted w.r.t. to the start time of each span
   */
  @Test
  void testOrdering() {


    final Timestamp start1 = new Timestamp(5L, 13);

    final Timestamp start2 = new Timestamp(5L, 0);
    final String traceId = "testtraceid";

    final EVSpan evSpan1 = EVSpan.newBuilder()
        .setLandscapeToken("tok")
        .setSpanId("1")
        .setTraceId(traceId)
        .setStartTime(start1)
        .setEndTime(20L)
        .setDuration(10L)
        .setOperationName("OpB")
        .setRequestCount(1)
        .setHostname("sampleHost")
        .setHostIpAddress("1.2.3.4")
        .setAppName("sampleapp")
        .setAppPid("1234")
        .setAppLanguage("lang")
        .build();

    final EVSpan evSpan2 = EVSpan.newBuilder()
        .setLandscapeToken("tok")
        .setSpanId("2")
        .setTraceId(traceId)
        .setStartTime(start2)
        .setEndTime(10L)
        .setDuration(5L)
        .setOperationName("OpA")
        .setRequestCount(1)
        .setHostname("sampleHost")
        .setHostIpAddress("1.2.3.4")
        .setAppName("sampleapp")
        .setAppPid("1234")
        .setAppLanguage("lang")
        .build();
    this.inputTopic.pipeInput(evSpan1.getTraceId(), evSpan1);
    this.inputTopic.pipeInput(evSpan2.getTraceId(), evSpan2);

    final Trace trace = this.outputTopic.readKeyValuesToList().get(1).value;

    // Trace must contain both spans
    assertEquals(2, trace.getSpanList().size());

    // Spans in span list must be sorted by start time

    final Instant instant1 = this.timestampToInstant(trace.getSpanList().get(0).getStartTime());
    final Instant instant2 = this.timestampToInstant(trace.getSpanList().get(1).getStartTime());

    assertTrue(instant1.isBefore(instant2));

  }


  /**
   * Tests whether a correct trace is generated based on only a single span
   */
  @Test
  void testTraceCreation() {
    final String traceId = "testtraceid";
    final Timestamp start = new Timestamp(10L, 0);

    final long end = this.timestampToInstant(start).toEpochMilli() + 17;

    final EVSpan span = EVSpan.newBuilder()
        .setLandscapeToken("tok")
        .setSpanId("1")
        .setTraceId(traceId)
        .setStartTime(start)
        .setEndTime(end)
        .setDuration(this.getDuration(start, end))
        .setOperationName("OpB")
        .setRequestCount(1)
        .setHostname("sampleHost")
        .setHostIpAddress("1.2.3.4")
        .setAppName("sampleapp")
        .setAppPid("1234")
        .setAppLanguage("lang")
        .build();

    this.inputTopic.pipeInput(span.getTraceId(), span);

    final Trace trace = this.outputTopic.readValue();
    assertNotNull(trace);

    assertEquals(traceId, trace.getTraceId());
    assertEquals(span.getDuration(), trace.getDuration());
    assertEquals(span.getStartTime(), trace.getStartTime());
    assertEquals(span.getEndTime(), trace.getEndTime());
    assertEquals(1, trace.getTraceCount());
    assertEquals(1, trace.getSpanList().size());

  }


  /**
   * Tests the windowing of traces. Spans with the same trace id in close temporal proximity should
   * be aggregated in the same trace. If another span with the same trace id arrives later, it
   * should not be included in the same trace.
   */
  @Test
  void testWindowing() {
    final String traceId = "testtraceid";

    final Timestamp start1 = new Timestamp(1584093875L, 0);

    final long end1 = 1584093891;

    final Timestamp start2 = new Timestamp(1584093875L, 2398423);
    final long end2 = 1584093893;

    final Timestamp start3 = new Timestamp(1584093875L + 7, 0);
    final long end3 = 1584093875L + 8;



    final EVSpan evSpan1 = EVSpan.newBuilder()
        .setLandscapeToken("tok")
        .setSpanId("1")
        .setTraceId(traceId)
        .setStartTime(start1)
        .setEndTime(end1)
        .setDuration(this.getDuration(start1, end1))
        .setOperationName("OpA")
        .setRequestCount(1)
        .setHostname("sampleHost")
        .setHostIpAddress("1.2.3.4")
        .setAppName("sampleapp")
        .setAppPid("1234")
        .setAppLanguage("lang")
        .build();

    final EVSpan evSpan2 = EVSpan.newBuilder()
        .setLandscapeToken("tok")
        .setSpanId("2")
        .setTraceId(traceId)
        .setStartTime(start2)
        .setEndTime(end2)
        .setDuration(this.getDuration(start2, end2))
        .setOperationName("OpB")
        .setRequestCount(1)
        .setHostname("sampleHost")
        .setHostIpAddress("1.2.3.4")
        .setAppName("sampleapp")
        .setAppPid("1234")
        .setAppLanguage("lang")
        .build();

    // This Span's timestamp is after closing the window containing
    // the first two spans
    final EVSpan evSpan3 = EVSpan.newBuilder()
        .setLandscapeToken("tok")
        .setSpanId("3")
        .setTraceId(traceId)
        .setStartTime(start3)
        .setEndTime(end3)
        .setDuration(this.getDuration(start3, end3))
        .setOperationName("OpC")
        .setRequestCount(1)
        .setHostname("sampleHost")
        .setHostIpAddress("1.2.3.4")
        .setAppName("sampleapp")
        .setAppPid("1234")
        .setAppLanguage("lang")
        .build();


    this.inputTopic.pipeInput(evSpan1.getTraceId(), evSpan1);
    this.inputTopic.pipeInput(evSpan2.getTraceId(), evSpan2);
    this.inputTopic.pipeInput(evSpan3.getTraceId(), evSpan3);

    assertEquals(3, this.outputTopic.getQueueSize());

    final List<KeyValue<String, Trace>> records = this.outputTopic.readKeyValuesToList();

    // First 'complete' Trace should encompass first two spans
    final Trace trace = records.get(1).value;
    assertEquals(start1, trace.getStartTime());
    assertEquals(end2, trace.getEndTime());
    assertEquals(1, trace.getTraceCount());
    assertEquals(2, trace.getSpanList().size());

    // Second trace should only include the last span and its values
    final Trace trace2 = records.get(2).value;
    assertEquals(start3, trace2.getStartTime());
    assertEquals(end3, trace2.getEndTime());
    assertEquals(1, trace2.getTraceCount());
    assertEquals(1, trace2.getSpanList().size());
  }



  /**
   * Spans with different trace id that are otherwise similar, should be reduced to a single trace
   */
  @Test
  void testTraceReduction() {
    final String operationName = "OpName";

    final Timestamp start1 = new Timestamp(10L, 0);
    final long end1 = 20L;

    final Timestamp start2 = new Timestamp(11L, 12983);
    final long end2 = 80L;

    final String firstTraceId = "trace1";

    final EVSpan evSpan1 = EVSpan.newBuilder()
        .setLandscapeToken("tok")
        .setSpanId("1")
        .setTraceId(firstTraceId)
        .setStartTime(start1)
        .setEndTime(end1)
        .setDuration(this.getDuration(start1, end1))
        .setOperationName(operationName)
        .setRequestCount(1)
        .setHostname("sampleHost")
        .setHostIpAddress("1.2.3.4")
        .setAppName("sampleapp")
        .setAppPid("1234")
        .setAppLanguage("lang")
        .build();

    final EVSpan evSpan2 = EVSpan.newBuilder()
        .setLandscapeToken("tok")
        .setSpanId("2")
        .setTraceId("trace2")
        .setStartTime(start2)
        .setEndTime(end2)
        .setDuration(this.getDuration(start2, end2))
        .setOperationName(operationName)
        .setRequestCount(265)
        .setHostname("sampleHost")
        .setHostIpAddress("1.2.3.4")
        .setAppName("sampleapp")
        .setAppPid("1234")
        .setAppLanguage("lang")
        .build();


    this.inputTopic.pipeInput(evSpan1.getTraceId(), evSpan1);
    this.inputTopic.pipeInput(evSpan2.getTraceId(), evSpan2);

    final List<KeyValue<String, Trace>> records = this.outputTopic.readKeyValuesToList();

    assertEquals(2, records.size());

    // Trace is "completed" after two updates, thus take the second record
    final Trace trace = records.get(1).value;

    assertEquals(start1, trace.getStartTime());
    assertEquals(end2, trace.getEndTime());

    // Reduction
    assertEquals(2, trace.getTraceCount());

    // Only the span list of the latest trace in the reduction should be used
    assertEquals(1, trace.getSpanList().size());
    assertEquals(265, trace.getSpanList().get(0).getRequestCount());

    // Trace id of the reduced trace should be equal to the trace id of the first trace
    assertEquals(firstTraceId, trace.getTraceId());

  }

  /**
   * Traces that were created within different windows should not be reduced to one another
   */
  @Test
  void testTraceReductionWindowing() {

    final String traceId1 = "trace1";
    final String traceId2 = "trace2";
    final String operationName = "OpName";

    final Timestamp start1 = new Timestamp(10L, 0);
    final long end1 = 20L;

    final Instant start1plusWindow = this.timestampToInstant(start1).plusSeconds(7);

    final Timestamp start2 =
        new Timestamp(start1plusWindow.getEpochSecond(), start1plusWindow.getNano());
    final long end2 = this.timestampToInstant(start1).plusMillis(100).toEpochMilli();


    final EVSpan evSpan1 = EVSpan.newBuilder()
        .setLandscapeToken("tok")
        .setSpanId("1")
        .setTraceId(traceId1)
        .setStartTime(start1)
        .setEndTime(end1)
        .setDuration(this.getDuration(start1, end1))
        .setOperationName(operationName)
        .setRequestCount(1)
        .setHostname("sampleHost")
        .setHostIpAddress("1.2.3.4")
        .setAppName("sampleapp")
        .setAppPid("1234")
        .setAppLanguage("lang")
        .build();
    final EVSpan evSpan2 = EVSpan.newBuilder()
        .setLandscapeToken("tok")
        .setSpanId("2")
        .setTraceId(traceId2)
        .setStartTime(start2)
        .setEndTime(end2)
        .setDuration(this.getDuration(start2, end2))
        .setOperationName(operationName)
        .setRequestCount(1)
        .setHostname("sampleHost")
        .setHostIpAddress("1.2.3.4")
        .setAppName("sampleapp")
        .setAppPid("1234")
        .setAppLanguage("lang")
        .build();

    this.inputTopic.pipeInput(evSpan1.getTraceId(), evSpan1);
    this.inputTopic.pipeInput(evSpan2.getTraceId(), evSpan2);

    final List<Trace> traces = this.outputTopic.readValuesToList();

    traces.forEach(System.out::println);

    assertEquals(traceId1, traces.get(0).getTraceId());
    assertEquals(traceId2, traces.get(1).getTraceId());

  }

  private Instant timestampToInstant(final Timestamp ts) {
    return Instant.ofEpochSecond(ts.getSeconds(), ts.getNanoAdjust());
  }

  private long getDuration(final Timestamp start, final long end) {
    return Duration.between(this.timestampToInstant(start), Instant.ofEpochMilli(end)).toNanos();
  }

}
