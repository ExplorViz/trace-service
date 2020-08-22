package net.explorviz.trace.service;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import net.explorviz.avro.SpanDynamic;
import net.explorviz.avro.Trace;
import net.explorviz.trace.TraceHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TraceReducerTest {

  private TraceReducer reducer;

  @BeforeEach
  void setUp() {
    reducer = new TraceReducer();
  }

  @Test
  void reduceLoop() {

    SpanDynamic s1 = TraceHelper.randomSpan();
    SpanDynamic s2 = SpanDynamic.newBuilder(s1)
        .setParentSpanId(s1.getParentSpanId())
        .setHashCode(s1.getHashCode()).build();
    SpanDynamic s3 = SpanDynamic.newBuilder(s1)
        .setParentSpanId(s1.getParentSpanId())
        .setHashCode(s1.getHashCode()).build();

    Trace trace = TraceHelper.randomTrace(1);
    trace.setSpanList(Arrays.asList(s1, s2, s3));

    Trace got = reducer.reduce(trace);

    assertEquals(1, got.getSpanList().size());
    assertEquals(s1, got.getSpanList().get(0));

  }

  @Test
  void reduceDirectRecursion() {
    SpanDynamic s1 = TraceHelper.randomSpan();
    SpanDynamic s2 = TraceHelper.randomSpan();
    SpanDynamic s3 = TraceHelper.randomSpan();
    s2.setHashCode(s1.getHashCode());
    s2.setParentSpanId(s1.getSpanId());
    s3.setHashCode(s1.getHashCode());
    s3.setParentSpanId(s2.getSpanId());
    /*
      s1 -> s2 -> s3
      all refer to the same method
     */

    Trace trace = TraceHelper.randomTrace(1);
    trace.setSpanList(Arrays.asList(s1, s2, s3));

    Trace got = reducer.reduce(trace);

    assertEquals(1, got.getSpanList().size());
    assertEquals(s1, got.getSpanList().get(0));
  }
}
