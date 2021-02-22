package net.explorviz.trace.persistence.cassandra;

import static io.restassured.RestAssured.when;
import static org.hamcrest.Matchers.notNullValue;
import com.datastax.oss.quarkus.test.CassandraTestResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.ws.rs.core.Response;
import net.explorviz.avro.Trace;
import net.explorviz.trace.service.TraceRepository;
import org.junit.jupiter.api.Test;

@QuarkusTest
@QuarkusTestResource(KafkaTestResource.class)
@QuarkusTestResource(CassandraTestResource.class)
@TestProfile(CassandraTestProfile.class)
public class TraceResourceIt {

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

    this.repository.insert(TraceHelper.randomTrace(2));

    TimeUnit.MINUTES.sleep(1);

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

    System.out.println("mytest " + actual.toString());

  }

}
