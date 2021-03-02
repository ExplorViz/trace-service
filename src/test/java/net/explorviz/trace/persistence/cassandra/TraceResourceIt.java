package net.explorviz.trace.persistence.cassandra;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import javax.inject.Inject;
import net.explorviz.trace.persistence.dao.Trace;
import net.explorviz.trace.service.TraceConverter;
import net.explorviz.trace.service.TraceRepository;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@QuarkusTest
@QuarkusTestResource(KafkaTestResource.class)
@QuarkusTestResource(CassandraCustomTestResource.class)
@TestProfile(CassandraTestProfile.class)
public class TraceResourceIt {

  private static final Logger LOGGER = Logger.getLogger(TraceResourceIt.class);

  // Tests
  // - insert and retrieve single trace with 5 spans
  // - insert and retrieve 5 traces with 5 spans each
  // - filter by timestamp
  // - get by trace id

  @Inject
  private TraceRepository repository;

  @Test
  public void shouldSaveAndRetrieveEntity() throws InterruptedException {

    final Trace expected =
        TraceConverter.convertTraceToDao(TraceHelper.randomTrace(2));


    LOGGER.info("test token " + expected.getLandscapeToken() + " and " + expected.getStartTime()
        + " and " + expected.getEndTime());


    this.repository.insert(expected).await().indefinitely();

    final Trace got =
        this.repository.getByTraceId(expected.getLandscapeToken(), expected.getTraceId())
            .collectItems().first().await().indefinitely();


    Assertions.assertEquals(expected, got);


  }

}
