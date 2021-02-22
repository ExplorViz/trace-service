package net.explorviz.trace.persistence.cassandra;

import static io.restassured.RestAssured.when;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import com.datastax.oss.quarkus.test.CassandraTestResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import javax.inject.Inject;
import javax.ws.rs.core.Response;
import net.explorviz.avro.Trace;
import net.explorviz.trace.service.TraceRepository;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

@QuarkusTest
@QuarkusTestResource(KafkaTestResource.class)
@QuarkusTestResource(CassandraTestResource.class)
@TestProfile(CassandraTestProfile.class)
public class TraceResourceIt {

  private static final Logger LOG = Logger.getLogger(TraceResourceIt.class);

  @Inject
  Logger logger;

  // Tests
  // - insert and retrieve single trace with 5 spans
  // - insert and retrieve 5 traces with 5 spans each
  // - filter by timestamp
  // - get by trace id

  @Inject
  private TraceRepository repository;

  @Test
  public void shouldSaveAndRetrieveEntity() throws InterruptedException {

    LOG.info("run my test");
    LOG.debug("run my test");
    LOG.trace("run my test");

    this.logger.info("run my test");
    this.logger.debug("run my test");
    this.logger.trace("run my test");


    final Trace expected = TraceHelper.randomTrace(2);

    this.repository.insert(expected);

    // TimeUnit.MINUTES.sleep(1);

    final net.explorviz.trace.persistence.dao.Trace[] actual =
        when()
            .get("/v2/landscapes/" + expected.getLandscapeToken() + "/dynamic")
            .then()
            .statusCode(Response.Status.OK.getStatusCode())
            .body(notNullValue())
            .extract()
            .body()
            .as(net.explorviz.trace.persistence.dao.Trace[].class);

    // assertThat(actual).contains(expected);

    assertEquals(1, actual.length);

    System.out.println("mytest " + actual.toString());

  }

}
