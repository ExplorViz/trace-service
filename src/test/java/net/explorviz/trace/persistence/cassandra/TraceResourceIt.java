package net.explorviz.trace.persistence.cassandra;

import static io.restassured.RestAssured.given;
import com.datastax.oss.quarkus.test.CassandraTestResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.response.Response;
import javax.inject.Inject;
import net.explorviz.avro.Trace;
import net.explorviz.trace.service.TraceRepository;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

@QuarkusTest
@QuarkusTestResource(KafkaTestResource.class)
@QuarkusTestResource(CassandraTestResource.class)
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

    final Trace expected = TraceHelper.randomTrace(2);

    LOGGER.info("test token " + expected.getLandscapeToken() + " and " + expected.getStartTime()
        + " and " + expected.getEndTime());

    this.repository.insert(expected);

    // TimeUnit.MINUTES.sleep(1);

    // final net.explorviz.trace.persistence.dao.Trace[] actual =
    // given().pathParam("landscapeToken", expected.getLandscapeToken()).when()
    // .get("/v2/landscapes/{landscapeToken}/dynamic")
    // .then()
    // .statusCode(Response.Status.OK.getStatusCode())
    // .body(notNullValue())
    // .extract()
    // .body()
    // .as(net.explorviz.trace.persistence.dao.Trace[].class);


    final Response response =
        given().pathParam("landscapeToken", expected.getLandscapeToken()).when()
            .get("/v2/landscapes/{landscapeToken}/dynamic");

    final String body = response.getBody().asPrettyString();

    LOGGER.info("test body " + body);

    // assertThat(actual).contains(expected);

    // assertEquals(1, actual.length);

  }

}
