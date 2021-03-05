package net.explorviz.trace.cassandra;

import static io.restassured.RestAssured.given;
import com.datastax.oss.quarkus.test.CassandraTestResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.response.Response;
import java.util.Arrays;
import java.util.List;
import javax.inject.Inject;
import net.explorviz.trace.persistence.dao.Trace;
import net.explorviz.trace.service.TraceConverter;
import net.explorviz.trace.service.TraceRepository;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


@QuarkusTest
@QuarkusTestResource(KafkaTestResource.class)
@QuarkusTestResource(CassandraTestResource.class)
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
  public void shouldSaveAndRetrieveEntityById() throws InterruptedException {

    final Trace expected =
        TraceConverter.convertTraceToDao(TraceHelper.randomTrace(5));

    this.repository.insert(expected).await().indefinitely();

    final Response response =
        given().pathParam("landscapeToken", expected.getLandscapeToken()).when()
            .get("/v2/landscapes/{landscapeToken}/dynamic");

    final net.explorviz.trace.persistence.dao.Trace[] body =
        response.getBody().as(net.explorviz.trace.persistence.dao.Trace[].class);

    Assertions.assertEquals(expected, body[0]);
  }

  @Test
  public void shouldSaveAndRetrieveAllEntitiesById() throws InterruptedException {

    final String landscapeToken1 = "a";
    final String landscapeToken2 = "b";

    final Trace expected1 =
        TraceConverter.convertTraceToDao(TraceHelper.randomTrace(5, landscapeToken1));
    final Trace expected2 =
        TraceConverter.convertTraceToDao(TraceHelper.randomTrace(5, landscapeToken1));
    final Trace expected3 =
        TraceConverter.convertTraceToDao(TraceHelper.randomTrace(5, landscapeToken1));
    final Trace remainder4 =
        TraceConverter.convertTraceToDao(TraceHelper.randomTrace(5, landscapeToken2));
    final Trace remainder5 =
        TraceConverter.convertTraceToDao(TraceHelper.randomTrace(5, landscapeToken2));

    this.repository.insert(expected1).await().indefinitely();
    this.repository.insert(expected2).await().indefinitely();
    this.repository.insert(expected3).await().indefinitely();
    this.repository.insert(remainder4).await().indefinitely();
    this.repository.insert(remainder5).await().indefinitely();

    final Response response =
        given().pathParam("landscapeToken", landscapeToken1).when()
            .get("/v2/landscapes/{landscapeToken}/dynamic");

    final net.explorviz.trace.persistence.dao.Trace[] body =
        response.getBody().as(net.explorviz.trace.persistence.dao.Trace[].class);

    final List<Trace> actualTraceList = Arrays.asList(body);

    Assertions.assertTrue(actualTraceList.size() == 3);

    Assertions.assertTrue(actualTraceList.contains(expected1));
    Assertions.assertTrue(actualTraceList.contains(expected2));
    Assertions.assertTrue(actualTraceList.contains(expected3));
  }

  @Test
  public void retrieveMultipleEntitiesByTimestamp() throws InterruptedException {

    final String landscapeToken1 = "c";
    final String landscapeToken2 = "d";

    final long fromSeconds1 = 1605700800L;
    final long toSeconds1 = 1605700810L;

    final long fromSeconds2 = 1605700811L;
    final long toSeconds2 = 1605700821L;

    final Trace expected1 =
        TraceConverter.convertTraceToDao(
            TraceHelper.randomTrace(5, landscapeToken1, fromSeconds1, toSeconds1));
    final Trace expected2 =
        TraceConverter.convertTraceToDao(
            TraceHelper.randomTrace(5, landscapeToken1, toSeconds1, fromSeconds2));
    final Trace remainder3 =
        TraceConverter.convertTraceToDao(
            TraceHelper.randomTrace(5, landscapeToken1, fromSeconds2, toSeconds2));
    final Trace remainder4 =
        TraceConverter.convertTraceToDao(
            TraceHelper.randomTrace(5, landscapeToken2, fromSeconds1, toSeconds1));
    final Trace remainder5 =
        TraceConverter.convertTraceToDao(
            TraceHelper.randomTrace(5, landscapeToken2, fromSeconds2, toSeconds2));

    final long from = expected1.getStartTime();
    final long to = expected2.getStartTime();

    this.repository.insert(expected1).await().indefinitely();
    this.repository.insert(expected2).await().indefinitely();
    this.repository.insert(remainder3).await().indefinitely();
    this.repository.insert(remainder4).await().indefinitely();
    this.repository.insert(remainder5).await().indefinitely();

    final Response response =
        given().pathParam("landscapeToken", landscapeToken1).queryParam("from", from)
            .queryParam("to", to).when()
            .get("/v2/landscapes/{landscapeToken}/dynamic");

    final net.explorviz.trace.persistence.dao.Trace[] body =
        response.getBody().as(net.explorviz.trace.persistence.dao.Trace[].class);

    final List<Trace> actualTraceList = Arrays.asList(body);

    Assertions.assertTrue(actualTraceList.size() == 2);

    Assertions.assertTrue(actualTraceList.contains(expected1));
    Assertions.assertTrue(actualTraceList.contains(expected2));
  }

  @Test
  public void testOutOfRangeByTimestamp() throws InterruptedException {

    final String landscapeToken1 = "c";
    final String landscapeToken2 = "d";

    final long fromSeconds1 = 1605700800L;
    final long toSeconds1 = 1605700810L;

    final long fromSeconds2 = 1605700811L;
    final long toSeconds2 = 1605700821L;

    final long outOfRangeFromMilli2 = 1605700822000L;

    final Trace remainder1 =
        TraceConverter.convertTraceToDao(
            TraceHelper.randomTrace(5, landscapeToken1, fromSeconds1, toSeconds1));
    final Trace remainder2 =
        TraceConverter.convertTraceToDao(
            TraceHelper.randomTrace(5, landscapeToken1, toSeconds1, fromSeconds2));
    final Trace remainder3 =
        TraceConverter.convertTraceToDao(
            TraceHelper.randomTrace(5, landscapeToken1, fromSeconds2, toSeconds2));
    final Trace remainder4 =
        TraceConverter.convertTraceToDao(
            TraceHelper.randomTrace(5, landscapeToken2, fromSeconds1, toSeconds1));
    final Trace remainder5 =
        TraceConverter.convertTraceToDao(
            TraceHelper.randomTrace(5, landscapeToken2, fromSeconds2, toSeconds2));

    this.repository.insert(remainder1).await().indefinitely();
    this.repository.insert(remainder2).await().indefinitely();
    this.repository.insert(remainder3).await().indefinitely();
    this.repository.insert(remainder4).await().indefinitely();
    this.repository.insert(remainder5).await().indefinitely();

    final Response response =
        given().pathParam("landscapeToken", landscapeToken1)
            .queryParam("from", outOfRangeFromMilli2).when()
            .get("/v2/landscapes/{landscapeToken}/dynamic");

    final net.explorviz.trace.persistence.dao.Trace[] body =
        response.getBody().as(net.explorviz.trace.persistence.dao.Trace[].class);

    Assertions.assertTrue(body.length == 0);
  }

  @Test
  public void getSingleTraceByTraceId() throws InterruptedException {

    final String landscapeToken = "e";

    final Trace expected =
        TraceConverter.convertTraceToDao(TraceHelper.randomTrace(5, landscapeToken));
    final Trace remainder =
        TraceConverter.convertTraceToDao(TraceHelper.randomTrace(5, landscapeToken));

    this.repository.insert(expected).await().indefinitely();
    this.repository.insert(remainder).await().indefinitely();

    final Response response =
        given().pathParam("landscapeToken", expected.getLandscapeToken())
            .pathParam("traceId", expected.getTraceId()).when()
            .get("/v2/landscapes/{landscapeToken}/dynamic/{traceId}");

    final net.explorviz.trace.persistence.dao.Trace[] body =
        response.getBody().as(net.explorviz.trace.persistence.dao.Trace[].class);

    Assertions.assertTrue(body.length == 1);

    Assertions.assertEquals(expected, body[0]);
  }

}
