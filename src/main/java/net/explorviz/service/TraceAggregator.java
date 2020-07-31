package net.explorviz.service;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import net.explorviz.avro.SpanDynamic;
import net.explorviz.avro.Timestamp;
import net.explorviz.avro.Trace;

/**
 * Contains methods that help to aggregate multiple span into a trace as they come in.
 */
public class TraceAggregator {



  private Trace initTrace(String traceId, Trace freshTrace, SpanDynamic firstSpan) {


    freshTrace.setTraceId(traceId);

    // Use linked list here to avoid costly reallocation of array lists
    // We don't need random access provided by array lists
    freshTrace.setSpanList(new LinkedList<>());
    freshTrace.getSpanList().add(firstSpan);

    // Set start and end time to equal to the times of the only span
    freshTrace.setStartTime(firstSpan.getStartTime());
    freshTrace.setEndTime(firstSpan.getEndTime());

    freshTrace.setDuration(millisBetween(firstSpan.getStartTime(), firstSpan.getEndTime()));

    freshTrace.setOverallRequestCount(1);
    freshTrace.setTraceCount(1);

    // set initial trace id - do not change, since this is the major key for kafka
    // partitioning
    freshTrace.setTraceId(freshTrace.getTraceId());

    return freshTrace;
  }

  /**
   * Adds a {@link SpanDynamic} to a given trace. Adjusts start and end times as well as requests
   * counts of the trace and takes care of new and empty traces.
   * Additionally makes sure that spans are ordered by their respective start times.
   *
   * @param traceId   the trace Id
   * @param aggregate the trace to add the span to
   * @param newSpan   the span to add to the trace
   * @return the trace with the span included
   */
  public Trace aggregate(String traceId, Trace aggregate, SpanDynamic newSpan) {

    if (aggregate.getSpanList() == null || aggregate.getSpanList().isEmpty()) {
      return initTrace(traceId, aggregate, newSpan);
    }

    // Add the span to the trace at the correct position
    insertSorted(aggregate.getSpanList(), newSpan);
    // Depending on the position the span was inserted, the start or end time must be adjusted
    if (isBefore(newSpan.getStartTime(), aggregate.getStartTime())) {
      // Span is the current earliest in the trace
      aggregate.setStartTime(newSpan.getStartTime());
    } else if (isAfter(newSpan.getEndTime(), aggregate.getEndTime())) {
      // Span is the current latest in the trace
      aggregate.setEndTime(newSpan.getEndTime());
    }

    return aggregate;
  }



  /**
   * Inserts a span to the span list such that spans are sorted by start time
   * @param spanList the list to add the span to
   * @param insertMe the span to insert
   * @return the position the span was inserted to
   */
  private int insertSorted(List<SpanDynamic> spanList, SpanDynamic insertMe) {
    int i = 0;
    while (i < spanList.size() && isBefore(spanList.get(i).getStartTime(), insertMe.getEndTime())) {
      i++;
    }
    spanList.add(i, insertMe);
    return i;
  }


  /**
   * Checks if the first timestamp is before than the second.
   *
   * @param one the first timestamp
   * @param two the second timestamp
   * @return true iff first < second
   */
  private boolean isBefore(Timestamp one, Timestamp two) {
    Instant f = Instant.ofEpochSecond(one.getSeconds(), one.getNanoAdjust());
    Instant s = Instant.ofEpochSecond(two.getSeconds(), two.getNanoAdjust());
    return f.isBefore(s);
  }

  /**
   * Checks if the first timestamp is after than the second.
   *
   * @param one the first timestamp
   * @param two the second timestamp
   * @return true iff first < second
   */
  private boolean isAfter(Timestamp one, Timestamp two) {
    Instant f = Instant.ofEpochSecond(one.getSeconds(), one.getNanoAdjust());
    Instant s = Instant.ofEpochSecond(two.getSeconds(), two.getNanoAdjust());
    return f.isAfter(s);
  }

  private long millisBetween(Timestamp start, Timestamp end) {
    Instant s = Instant.ofEpochSecond(start.getSeconds(), start.getNanoAdjust());
    Instant e = Instant.ofEpochSecond(end.getSeconds(), end.getNanoAdjust());
    return Duration.between(s, e).toMillis();
  }

}
