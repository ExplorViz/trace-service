package net.explorviz.service;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import net.explorviz.avro.SpanDynamic;
import net.explorviz.avro.Timestamp;
import net.explorviz.avro.Trace;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SpanToTraceAggregatorTest {

  private static final String TEST_TOKEN = "tok";

  private SpanToTraceAggregator aggregator;

  @BeforeEach
  void setUp() {
    aggregator = new SpanToTraceAggregator();
  }

  @Test
  void newTrace() {
    Instant now = Instant.now();
    SpanDynamic fresh = SpanDynamic.newBuilder()
        .setLandscapeToken("tok")
        .setStartTime(toTimestamp(now.minus(1, ChronoUnit.SECONDS)))
        .setEndTime(toTimestamp(now))
        .setTraceId("tid")
        .setHashCode("hash")
        .build();

    Trace trace = new Trace();

    Trace traceWithSpan = aggregator.aggregate("", trace, fresh);

    assertEquals(1, trace.getSpanList().size(), "Invalid amount of spans in trace");
    assertTrue(trace.getSpanList().contains(fresh), "Trace does not contain first span");
    assertEquals(fresh.getStartTime(), trace.getStartTime(), "Start time does not match");
    assertEquals(fresh.getEndTime(), trace.getEndTime(), "End time does not match");
  }

  void addEarlierSpan() {
    Instant now = Instant.now();
    SpanDynamic first = SpanDynamic.newBuilder()
        .setLandscapeToken("tok")
        .setStartTime(toTimestamp(now.minus(1, ChronoUnit.SECONDS)))
        .setEndTime(toTimestamp(now))
        .setTraceId("tid")
        .setHashCode("hash")
        .build();

    Trace trace = new Trace();
    trace.setSpanList(new ArrayList<>());
    trace.getSpanList().add(first);

    SpanDynamic newFirst = SpanDynamic.newBuilder(first)
        .setStartTime(toTimestamp(now.minus(1, ChronoUnit.SECONDS)))
        .build();
    aggregator.aggregate("", trace, newFirst);
    assertEquals(2, trace.getSpanList().size(), "Invalid amount of spans in trace");
    assertEquals(trace.getSpanList().get(0), newFirst,"Trace does not contain first span");
  }

  void addLaterSpan() {
    Instant now = Instant.now();
    SpanDynamic first = SpanDynamic.newBuilder()
        .setLandscapeToken("tok")
        .setStartTime(toTimestamp(now.minus(1, ChronoUnit.SECONDS)))
        .setEndTime(toTimestamp(now))
        .setTraceId("tid")
        .setHashCode("hash")
        .build();

    Trace trace = new Trace();
    trace.setSpanList(new ArrayList<>());
    trace.getSpanList().add(first);

    SpanDynamic newLast = SpanDynamic.newBuilder(first)
        .setStartTime(toTimestamp(now.plus(1, ChronoUnit.SECONDS)))
        .setEndTime(toTimestamp(now.plus(5, ChronoUnit.SECONDS)))
        .build();
    aggregator.aggregate("", trace, newLast);
    assertEquals(2, trace.getSpanList().size(), "Invalid amount of spans in trace");
    assertEquals(trace.getSpanList().get(1), newLast,"Trace does not contain first span");
  }



  private Timestamp toTimestamp(Instant instant) {
    return new Timestamp(instant.getEpochSecond(), instant.getNano());
  }


}
