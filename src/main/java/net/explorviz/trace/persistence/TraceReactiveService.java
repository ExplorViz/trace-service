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

  public Multi<Trace> getAllAsync(final String id) {
    return this.traceDaoReactive.getAllAsync(id);
  }

  public Multi<Trace> getByStartTimeAndEndTime(final String id, final long startTime,
      final long endTime) {
    return this.traceDaoReactive.getByStartTimeAndEndTime(id, startTime, endTime);
  }

  public Multi<Trace> getByTraceId(final String id, final String traceId) {
    return this.traceDaoReactive.getByTraceId(id, traceId);
  }

}
