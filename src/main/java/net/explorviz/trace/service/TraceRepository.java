package net.explorviz.trace.service;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import net.explorviz.trace.persistence.TraceReactiveService;
import net.explorviz.trace.persistence.dao.Trace;

/**
 * Repository to manage instances of a {@link Trace}.
 */
@ApplicationScoped
public class TraceRepository {

  private final TraceReactiveService traceReactiveService;

  @Inject
  public TraceRepository(final TraceReactiveService traceReactiveService) {
    this.traceReactiveService = traceReactiveService;
  }

  public Uni<Void> insert(final net.explorviz.avro.Trace t) {
    return this.traceReactiveService.insert(TraceConverter.convertTraceToDao(t));
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

}
