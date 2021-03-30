package net.explorviz.trace.persistence;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import net.explorviz.trace.persistence.dao.Trace;
import net.explorviz.trace.persistence.dao.TraceDaoReactive;

/**
 * Business layer service to store/load {@link Trace} from the Cassandra database.
 */
@ApplicationScoped
public class TraceReactiveService {

  private final TraceDaoReactive traceDaoReactive;

  @Inject
  public TraceReactiveService(final TraceDaoReactive traceDaoReactive) {
    this.traceDaoReactive = traceDaoReactive;
  }

  public Uni<Void> insert(final Trace trace) {
    return this.traceDaoReactive.insertAsync(trace);
  }

  public Multi<Trace> getAllAsync(final String landscapeToken) {
    return this.traceDaoReactive.getAllAsync(landscapeToken);
  }

  public Multi<Trace> getByStartTimeAndEndTime(final String landscapeToken, final long startTime,
      final long endTime) {
    return this.traceDaoReactive.getByStartTimeAndEndTime(landscapeToken, startTime, endTime);
  }

  public Multi<Trace> getByTraceId(final String landscapeToken, final String traceId) {
    return this.traceDaoReactive.getByTraceId(landscapeToken, traceId);
  }
}
