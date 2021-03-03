package net.explorviz.trace.persistence.cassandra;

import static io.restassured.RestAssured.given;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.response.Response;
import javax.inject.Inject;
import net.explorviz.trace.persistence.dao.Trace;
import net.explorviz.trace.service.TraceConverter;
import net.explorviz.trace.service.TraceRepository;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


@QuarkusTest
@QuarkusTestResource(KafkaTestResource.class)
@QuarkusTestResource(CassandraCustomTestResource.class)
@TestProfile(CassandraTestProfile.class)
public class TraceResourceApi {

  // private static final Logger LOGGER = Logger.getLogger(TraceResourceIt.class);

  // Tests
  // - insert and retrieve single trace with 5 spans
  // - insert and retrieve 5 traces with 5 spans each
  // - filter by timestamp
  // - get by trace id

  @Inject
  TraceRepository repository;

  @Test
  public void shouldSaveAndRetrieveEntity() throws InterruptedException {

    final Trace expected =
        TraceConverter.convertTraceToDao(TraceHelper.randomTrace(5));

    final Trace[] expectedArray = new Trace[] {expected};

    this.repository.insert(expected).await().indefinitely();

    // final net.explorviz.trace.persistence.dao.Trace[] actual =
    // given().pathParam("landscapeToken", expected.getLandscapeToken()).when()
    // .get("/v2/landscapes/{landscapeToken}/dynamic")
    // .then()
    // .statusCode(Response.Status.OK.getStatusCode())
    // .body(Matchers.notNullValue())
    // .extract()
    // .body()
    // .as(net.explorviz.trace.persistence.dao.Trace[].class);

    final Response response =
        given().pathParam("landscapeToken", expected.getLandscapeToken()).when()
            .get("/v2/landscapes/{landscapeToken}/dynamic");

    final net.explorviz.trace.persistence.dao.Trace[] body =
        response.getBody().as(net.explorviz.trace.persistence.dao.Trace[].class);

    Assertions.assertEquals(expected, body[0]);
  }

}
