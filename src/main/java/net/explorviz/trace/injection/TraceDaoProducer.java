package net.explorviz.trace.injection;

import com.datastax.oss.quarkus.runtime.api.session.QuarkusCqlSession;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import net.explorviz.trace.persistence.dao.TraceDaoReactive;
import net.explorviz.trace.persistence.dao.TraceMapper;
import net.explorviz.trace.persistence.dao.TraceMapperBuilder;

/**
 * Factory / Producer for the {@link TraceDaoReactive}.
 */
public class TraceDaoProducer {

  private final TraceDaoReactive spanDynamicDaoReactive;

  @Inject
  public TraceDaoProducer(final QuarkusCqlSession session) {

    // create a mapper
    final TraceMapper mapper = new TraceMapperBuilder(session).build();

    // instantiate our Daos
    spanDynamicDaoReactive = mapper.traceDaoReactive();
  }

  @Produces // NOPMD
  @ApplicationScoped
  /* default */ TraceDaoReactive produceSpanDynamicDaoReactive() {
    return spanDynamicDaoReactive;
  }

}
