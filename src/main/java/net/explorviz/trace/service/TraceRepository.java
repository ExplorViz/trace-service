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

  public Multi<Trace> getAllAsync(final String landscapeToken) {
    return this.traceReactiveService.getAllAsync(landscapeToken);
  }

  public Multi<Trace> getByStartTimeAndEndTime(final String landscapeToken, final long startTime,
      final long endTime) {
    return this.traceReactiveService.getByStartTimeAndEndTime(landscapeToken, startTime, endTime);
  }

  public Multi<Trace> getByTraceId(final String landscapeToken, final String traceId) {
    return this.traceReactiveService.getByTraceId(landscapeToken, traceId);
  }

  public Multi<Trace> cloneAllAsync(final String landscapeToken, final String clonedLandscapeToken) {
    return this.traceReactiveService.getAllAsync(clonedLandscapeToken).invoke(x -> x.setLandscapeToken(landscapeToken)).invoke(this::insert);
  }

}
