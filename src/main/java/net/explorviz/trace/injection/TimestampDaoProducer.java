package net.explorviz.trace.injection;

import com.datastax.oss.quarkus.runtime.api.session.QuarkusCqlSession;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import net.explorviz.trace.persistence.dao.TimestampDaoReactive;
import net.explorviz.trace.persistence.dao.TimestampMapper;
import net.explorviz.trace.persistence.dao.TimestampMapperBuilder;

/**
 * Factory / Producer for the {@link TimestampDaoReactive}.
 */
public class TimestampDaoProducer {

  private final TimestampDaoReactive timestampDaoReactive;

  @Inject
  public TimestampDaoProducer(final QuarkusCqlSession session) {

    // create a mapper
    final TimestampMapper mapper = new TimestampMapperBuilder(session).build();

    // instantiate our Daos
    timestampDaoReactive = mapper.timestampDaoReactive();
  }

  @Produces // NOPMD
  @ApplicationScoped
  /* default */ TimestampDaoReactive produceTimestampDaoReactive() {
    return timestampDaoReactive;
  }

}
