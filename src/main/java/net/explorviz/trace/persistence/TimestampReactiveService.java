package net.explorviz.trace.persistence;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import net.explorviz.trace.persistence.dao.Timestamp;
import net.explorviz.trace.persistence.dao.TimestampDaoReactive;

@ApplicationScoped
public class TimestampReactiveService {

  private final TimestampDaoReactive timestampDaoReactive;

  @Inject
  public TimestampReactiveService(TimestampDaoReactive timestampDaoReactive) {
    this.timestampDaoReactive = timestampDaoReactive;
  }

  public Uni<Void> add(Timestamp timestamp) {
    return timestampDaoReactive.updateAsync(timestamp);
  }

  public Multi<Timestamp> get(int id) {
    return timestampDaoReactive.findByIdAsync(id);
  }

}
