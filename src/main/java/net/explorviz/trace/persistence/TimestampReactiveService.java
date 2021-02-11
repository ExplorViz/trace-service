package net.explorviz.trace.persistence;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import net.explorviz.trace.persistence.dao.Timestamp;
import net.explorviz.trace.persistence.dao.TimestampDaoReactive;

/**
 * Business layer service to store/load {@link Timestamp} from the Cassandra database.
 */
@ApplicationScoped
public class TimestampReactiveService {

  private final TimestampDaoReactive timestampDaoReactive;

  @Inject
  public TimestampReactiveService(final TimestampDaoReactive timestampDaoReactive) {
    this.timestampDaoReactive = timestampDaoReactive;
  }

  public Uni<Void> add(final Timestamp timestamp) {
    return timestampDaoReactive.updateAsync(timestamp);
  }

  public Multi<Timestamp> get(final int id) {
    return timestampDaoReactive.findByIdAsync(id);
  }

}
