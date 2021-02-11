package net.explorviz.trace.injection;

import com.datastax.oss.quarkus.runtime.api.session.QuarkusCqlSession;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import net.explorviz.trace.persistence.dao.SpanDynamicDaoReactive;
import net.explorviz.trace.persistence.dao.SpanDynamicMapper;
import net.explorviz.trace.persistence.dao.SpanDynamicMapperBuilder;
import net.explorviz.trace.persistence.dao.TimestampDaoReactive;

/**
 * Factory / Producer for the {@link TimestampDaoReactive}.
 */
public class SpanDynamicDaoProducer {

  private final SpanDynamicDaoReactive spanDynamicDaoReactive;

  @Inject
  public SpanDynamicDaoProducer(final QuarkusCqlSession session) {

    // create a mapper
    final SpanDynamicMapper mapper = new SpanDynamicMapperBuilder(session).build();

    // instantiate our Daos
    spanDynamicDaoReactive = mapper.spanDynamicDaoReactive();
  }

  @Produces // NOPMD
  @ApplicationScoped
  /* default */ SpanDynamicDaoReactive produceSpanDynamicDaoReactive() {
    return spanDynamicDaoReactive;
  }

}
