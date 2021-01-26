package net.explorviz.trace.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import net.explorviz.avro.SpanDynamic;
import net.explorviz.avro.Timestamp;
import net.explorviz.avro.Trace;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TraceAggregatorTest {

  private static final String TEST_TOKEN = "tok";

  private TraceAggregator aggregator;

  @BeforeEach
  void setUp() {
    this.aggregator = new TraceAggregator();
  }

  @Test
  void newTrace() {
    final Instant now = Instant.now();
    final SpanDynamic fresh = SpanDynamic.newBuilder()
        .setLandscapeToken(TEST_TOKEN)
        .setSpanId("sid")
        .setStartTime(this.toTimestamp(now.minus(1, ChronoUnit.SECONDS)))
        .setEndTime(this.toTimestamp(now))
        .setTraceId("tid")
        .setHashCode("hash")
        .build();



    final Trace traceWithSpan = this.aggregator.aggregate(new Trace(), fresh);

    assertEquals(1, traceWithSpan.getSpanList().size(), "Invalid amount of spans in trace");
    assertTrue(traceWithSpan.getSpanList().contains(fresh), "Trace does not contain first span");
    assertEquals(traceWithSpan.getStartTime(), traceWithSpan.getStartTime(),
        "Start time does not match");
    assertEquals(traceWithSpan.getEndTime(), traceWithSpan.getEndTime(), "End time does not match");
  }

  @Test
  void addEarlierSpan() {
    final Instant now = Instant.now();
    final SpanDynamic first = SpanDynamic.newBuilder()
        .setLandscapeToken(TEST_TOKEN)
        .setStartTime(this.toTimestamp(now.minus(1, ChronoUnit.SECONDS)))
        .setEndTime(this.toTimestamp(now))
        .setTraceId("tid")
        .setSpanId("sid")
        .setHashCode("hash")
        .build();


    final Trace aggregate = this.aggregator.aggregate(new Trace(), first);

    final SpanDynamic newFirst = SpanDynamic.newBuilder(first)
        .setStartTime(this.toTimestamp(now.minus(1, ChronoUnit.SECONDS)))
        .build();
    this.aggregator.aggregate(aggregate, newFirst);
    assertEquals(2, aggregate.getSpanList().size(), "Invalid amount of spans in trace");
    assertEquals(aggregate.getSpanList().get(0), newFirst, "Trace does not contain first span");
  }

  @Test
  void addLaterSpan() {
    final Instant now = Instant.now();
    final SpanDynamic first = SpanDynamic.newBuilder()
        .setLandscapeToken(TEST_TOKEN)
        .setStartTime(this.toTimestamp(now.minus(1, ChronoUnit.SECONDS)))
        .setEndTime(this.toTimestamp(now))
        .setTraceId("tid")
        .setSpanId("sid")
        .setHashCode("hash")
        .build();

    final Trace aggregate = this.aggregator.aggregate(new Trace(), first);

    final SpanDynamic newLast = SpanDynamic.newBuilder(first)
        .setStartTime(this.toTimestamp(now.plus(1, ChronoUnit.SECONDS)))
        .setEndTime(this.toTimestamp(now.plus(5, ChronoUnit.SECONDS)))
        .build();
    this.aggregator.aggregate(aggregate, newLast);
    assertEquals(2, aggregate.getSpanList().size(), "Invalid amount of spans in trace");
    assertEquals(aggregate.getSpanList().get(1), newLast, "Trace does not contain first span");
  }



  private Timestamp toTimestamp(final Instant instant) {
    return new Timestamp(instant.getEpochSecond(), instant.getNano());
  }


}
