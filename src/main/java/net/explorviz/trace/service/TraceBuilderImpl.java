package net.explorviz.trace.service;

import static net.explorviz.trace.service.TimestampHelper.isAfter;
import static net.explorviz.trace.service.TimestampHelper.isBefore;
import java.util.ArrayList;
import java.util.Collection;
import javax.inject.Singleton;
import net.explorviz.avro.SpanDynamic;
import net.explorviz.avro.Timestamp;
import net.explorviz.avro.Trace;

/**
 * Builds a trace out of a set spans.
 */
@Singleton
public class TraceBuilderImpl implements TraceBuilder {


  @Override
  public Trace build(final Collection<SpanDynamic> spans) {
    if (spans.isEmpty()) {
      throw new IllegalArgumentException("No spans given");
    }
    final SpanDynamic s = spans.stream().findAny().get();

    final Trace.Builder builder = Trace.newBuilder();

    builder.setOverallRequestCount(1);
    builder.setTraceCount(1);
    builder.setLandscapeToken(s.getLandscapeToken());
    builder.setTraceId(s.getTraceId());
    builder.setStartTime(s.getStartTime());
    builder.setEndTime(s.getEndTime());

    // Update timings, check integrity
    spans.forEach(span -> {
      if (!span.getLandscapeToken().equals(builder.getLandscapeToken())) { // NOPMD
        throw new IllegalArgumentException("Ambiguous landscape tokens");
      } else if (!span.getTraceId().equals(builder.getTraceId())) {
        throw new IllegalArgumentException("Ambiguous trace ids");
      }
      if (isBefore(span.getStartTime(), builder.getStartTime())) {
        // Span is the current earliest in the trace
        builder.setStartTime(span.getStartTime());
      }
      if (isAfter(span.getEndTime(), builder.getEndTime())) {
        // Span is the current latest in the trace
        builder.setEndTime(span.getEndTime());
      }
    });

    builder.setSpanList(new ArrayList<>(spans));
    final Timestamp start = builder.getStartTime();
    final Timestamp end = builder.getEndTime();
    builder.setDuration(TimestampHelper.durationMs(start, end));

    return builder.build();
  }


}
