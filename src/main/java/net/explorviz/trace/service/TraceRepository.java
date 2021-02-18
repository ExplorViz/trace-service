package net.explorviz.trace.service;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;
import net.explorviz.avro.SpanDynamic;
import net.explorviz.trace.persistence.TraceReactiveService;
import net.explorviz.trace.persistence.dao.Trace;

public class TraceRepository {

  private final TraceReactiveService traceReactiveService;

  @Inject
  public TraceRepository(final TraceReactiveService traceReactiveService) {
    this.traceReactiveService = traceReactiveService;
  }

  public Uni<Void> insert(final net.explorviz.avro.Trace t) {
    return this.traceReactiveService.insert(this.convertTraceToDao(t));
  }

  public Uni<Void> insert(final Trace daoTrace) {
    return this.traceReactiveService.insert(daoTrace);
  }

  public Multi<Trace> getAllAsync(final String id) {
    return this.traceReactiveService.getAllAsync(id);
  }

  public Multi<Trace> getByStartTimeAndEndTime(final String id, final long startTime,
      final long endTime) {
    return this.traceReactiveService.getByStartTimeAndEndTime(id, startTime, endTime);
  }

  public Multi<Trace> getByTraceId(final String id, final String traceId) {
    return this.traceReactiveService.getByTraceId(id, traceId);
  }

  private Trace convertTraceToDao(final net.explorviz.avro.Trace t) {
    // Build Dao SpanList

    final List<net.explorviz.trace.persistence.dao.SpanDynamic> daoSpanList = new ArrayList<>();

    for (final SpanDynamic span : t.getSpanList()) {

      final long startTime = TimestampHelper.toInstant(span.getStartTime()).toEpochMilli();
      final long endTime = TimestampHelper.toInstant(span.getEndTime()).toEpochMilli();

      final net.explorviz.trace.persistence.dao.SpanDynamic spanDynamicEntity =
          new net.explorviz.trace.persistence.dao.SpanDynamic(span.getLandscapeToken(),
              span.getSpanId(), span.getParentSpanId(), span.getTraceId(), startTime,
              endTime, span.getHashCode());

      daoSpanList.add(spanDynamicEntity);
    }

    // Build Dao Trace

    final long startTime = TimestampHelper.toInstant(t.getStartTime()).toEpochMilli();
    final long endTime = TimestampHelper.toInstant(t.getEndTime()).toEpochMilli();

    return new net.explorviz.trace.persistence.dao.Trace(t.getLandscapeToken(), t.getTraceId(),
        startTime, endTime, t.getDuration(), t.getOverallRequestCount(),
        t.getTraceCount(), daoSpanList);
  }

}
