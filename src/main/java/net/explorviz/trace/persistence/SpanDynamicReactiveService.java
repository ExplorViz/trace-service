package net.explorviz.trace.persistence;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import net.explorviz.trace.persistence.dao.SpanDynamic;
import net.explorviz.trace.persistence.dao.SpanDynamicDaoReactive;

/**
 * Business layer service to store/load {@link SpanDynamic} from the Cassandra database.
 */
@ApplicationScoped
public class SpanDynamicReactiveService {

  private final SpanDynamicDaoReactive spanDynamicDaoReactive;

  @Inject
  public SpanDynamicReactiveService(final SpanDynamicDaoReactive spanDynamicDaoReactive) {
    this.spanDynamicDaoReactive = spanDynamicDaoReactive;
  }

  public Uni<Void> add(final SpanDynamic spanDynamic) {
    return spanDynamicDaoReactive.updateAsync(spanDynamic);
  } 

  public Multi<SpanDynamic> get(final String id) {
    return spanDynamicDaoReactive.findByIdAsync(id);
  }

}
