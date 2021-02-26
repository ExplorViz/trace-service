package net.explorviz.trace.persistence.cassandra;

import static io.restassured.RestAssured.given;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.response.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import net.explorviz.avro.Trace;
import net.explorviz.trace.persistence.dao.SpanDynamic;
import net.explorviz.trace.service.TimestampHelper;
import net.explorviz.trace.service.TraceRepository;
import org.jboss.logging.Logger;
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

    final Trace expected = TraceHelper.randomTrace(2);

    LOGGER.info("test token " + expected.getLandscapeToken() + " and " + expected.getStartTime()
        + " and " + expected.getEndTime());

    TimeUnit.MINUTES.sleep(2);

    this.repository.insert(expected);

    final net.explorviz.trace.persistence.dao.Trace expectedTrace =
        new net.explorviz.trace.persistence.dao.Trace();
    expectedTrace.setLandscapeToken(expected.getLandscapeToken());
    expectedTrace.setTraceId(expected.getTraceId());
    expectedTrace.setStartTime(TimestampHelper.toInstant(expected.getStartTime()).toEpochMilli());
    expectedTrace.setEndTime(TimestampHelper.toInstant(expected.getEndTime()).toEpochMilli());
    expectedTrace.setDuration(expected.getDuration());
    expectedTrace.setOverallRequestCount(expected.getOverallRequestCount());
    expectedTrace.setTraceCount(expected.getTraceCount());

    final net.explorviz.avro.SpanDynamic testObjectSpan = expected.getSpanList().get(0);

    final SpanDynamic expectedSpan = new SpanDynamic();
    expectedSpan.setLandscapeToken(testObjectSpan.getLandscapeToken());
    expectedSpan.setTraceId(testObjectSpan.getTraceId());
    expectedSpan.setSpanId(testObjectSpan.getSpanId());
    expectedSpan.setParentSpanId(testObjectSpan.getParentSpanId());
    expectedSpan
        .setStartTime(TimestampHelper.toInstant(testObjectSpan.getStartTime()).toEpochMilli());
    expectedSpan.setEndTime(TimestampHelper.toInstant(testObjectSpan.getEndTime()).toEpochMilli());
    expectedSpan.setHashCode(testObjectSpan.getHashCode());

    final List<SpanDynamic> expectedSpanList = new ArrayList<>();
    expectedSpanList.add(expectedSpan);

    expectedTrace.setSpanList(expectedSpanList);

    this.repository.insert(expectedTrace);

    LOGGER.info("Inserted trace");

    TimeUnit.MINUTES.sleep(5);

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

    TimeUnit.MINUTES.sleep(5);

    // assertThat(actual).contains(expected);

    // assertEquals(1, actual.length);

  }

}
