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

  public Uni<Void> add(final Trace trace) {
    return traceDaoReactive.updateAsync(trace);
  } 

  public Multi<Trace> get(final String id) {
    return traceDaoReactive.findByIdAsync(id);
  }

}
