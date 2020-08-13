package net.explorviz.trace.service;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayDeque;
import net.explorviz.avro.SpanDynamic;
import net.explorviz.avro.Trace;
import net.explorviz.trace.TraceHelper;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TraceBuilderImplTest {

  private TraceBuilderImpl builder;

  @BeforeEach
  void setUp() {
    builder = new TraceBuilderImpl();
  }

  @Test
  void buildValid() {
    Trace testTrace = TraceHelper.randomTrace(10);
    Trace got = builder.build(testTrace.getSpanList());

    testTrace.getSpanList().sort((i,j) -> StringUtils.compare(i.getSpanId(), j.getSpanId()));
    got.getSpanList().sort((i,j) -> StringUtils.compare(i.getSpanId(), j.getSpanId()));
    assertEquals(testTrace, got);
  }

  @Test
  void buildInvalidToken() {
    Trace testTrace = TraceHelper.randomTrace(10);
    SpanDynamic invalid = TraceHelper.randomSpan(testTrace.getTraceId(), "sometoken");
    testTrace.getSpanList().add(invalid);
    assertThrows(IllegalArgumentException.class, () -> builder.build(testTrace.getSpanList()));
  }

  @Test
  void buildInvalidTraceId() {
    Trace testTrace = TraceHelper.randomTrace(10);
    SpanDynamic invalid = TraceHelper.randomSpan("sometraceid", testTrace.getLandscapeToken());
    testTrace.getSpanList().add(invalid);
    assertThrows(IllegalArgumentException.class, () -> builder.build(testTrace.getSpanList()));
  }


  @Test
  void buildEmptySpans() {
    assertThrows(IllegalArgumentException.class, () -> builder.build(new ArrayDeque<>()));
  }




}
