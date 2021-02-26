package net.explorviz.trace.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.util.ArrayList;
import java.util.List;
import net.explorviz.trace.persistence.cassandra.TraceHelper;
import net.explorviz.trace.persistence.dao.SpanDynamic;
import net.explorviz.trace.persistence.dao.Trace;
import org.junit.jupiter.api.Test;

public class TraceConverterTest {

  @Test
  void testSimpleTraceConversion() {

    final net.explorviz.avro.Trace testObject = TraceHelper.randomTrace(1);
    final net.explorviz.avro.SpanDynamic testObjectSpan = testObject.getSpanList().get(0);

    final Trace expected = new Trace();
    expected.setLandscapeToken(testObject.getLandscapeToken());
    expected.setTraceId(testObject.getTraceId());
    expected.setStartTime(TimestampHelper.toInstant(testObject.getStartTime()).toEpochMilli());
    expected.setEndTime(TimestampHelper.toInstant(testObject.getEndTime()).toEpochMilli());
    expected.setDuration(testObject.getDuration());
    expected.setOverallRequestCount(testObject.getOverallRequestCount());
    expected.setTraceCount(testObject.getTraceCount());

    final SpanDynamic expectedSpan = new SpanDynamic();
    expectedSpan.setLandscapeToken(testObjectSpan.getLandscapeToken());
    expectedSpan.setTraceId(testObjectSpan.getTraceId());
    expectedSpan.setSpanId(testObjectSpan.getSpanId());
    expectedSpan.setParentSpanId(testObjectSpan.getParentSpanId());
    expectedSpan
        .setStartTime(TimestampHelper.toInstant(testObjectSpan.getStartTime()).toEpochMilli());
    expectedSpan.setEndTime(TimestampHelper.toInstant(testObjectSpan.getEndTime()).toEpochMilli());
    expectedSpan.setHashCode(testObjectSpan.getHashCode());

    final List<SpanDynamic> expectedSpanList = new ArrayList<>();
    expectedSpanList.add(expectedSpan);

    expected.setSpanList(expectedSpanList);

    final Trace result = TraceConverter.convertTraceToDao(testObject);

    assertEquals(expected, result);
  }

  @Test
  void testComplexTraceConversion() {

    final net.explorviz.avro.Trace testObject = TraceHelper.randomTrace(20);

    final Trace expected = new Trace();
    expected.setLandscapeToken(testObject.getLandscapeToken());
    expected.setTraceId(testObject.getTraceId());
    expected.setStartTime(TimestampHelper.toInstant(testObject.getStartTime()).toEpochMilli());
    expected.setEndTime(TimestampHelper.toInstant(testObject.getEndTime()).toEpochMilli());
    expected.setDuration(testObject.getDuration());
    expected.setOverallRequestCount(testObject.getOverallRequestCount());
    expected.setTraceCount(testObject.getTraceCount());

    final List<SpanDynamic> expectedSpanList = new ArrayList<>();

    for (final net.explorviz.avro.SpanDynamic testObjectSpan : testObject.getSpanList()) {

      final SpanDynamic expectedSpan = new SpanDynamic();
      expectedSpan.setLandscapeToken(testObjectSpan.getLandscapeToken());
      expectedSpan.setTraceId(testObjectSpan.getTraceId());
      expectedSpan.setSpanId(testObjectSpan.getSpanId());
      expectedSpan.setParentSpanId(testObjectSpan.getParentSpanId());
      expectedSpan
          .setStartTime(TimestampHelper.toInstant(testObjectSpan.getStartTime()).toEpochMilli());
      expectedSpan
          .setEndTime(TimestampHelper.toInstant(testObjectSpan.getEndTime()).toEpochMilli());
      expectedSpan.setHashCode(testObjectSpan.getHashCode());

      expectedSpanList.add(expectedSpan);
    }

    expected.setSpanList(expectedSpanList);

    final Trace result = TraceConverter.convertTraceToDao(testObject);

    assertEquals(expected, result);
  }


}
