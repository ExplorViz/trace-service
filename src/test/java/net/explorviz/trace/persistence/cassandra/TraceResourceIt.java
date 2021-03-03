package net.explorviz.trace.persistence.cassandra;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;
import net.explorviz.trace.persistence.dao.Trace;
import net.explorviz.trace.service.TraceConverter;
import net.explorviz.trace.service.TraceRepository;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@QuarkusTest
@QuarkusTestResource(KafkaTestResource.class)
@QuarkusTestResource(CassandraCustomTestResource.class)
@TestProfile(CassandraTestProfile.class)
public class TraceResourceIt {

  // private static final Logger LOGGER = Logger.getLogger(TraceResourceIt.class);

  // Tests
  // - insert and retrieve single trace with 5 spans
  // - insert and retrieve 5 traces with 5 spans each
  // - filter by timestamp
  // - get by trace id

  @Inject
  private TraceRepository repository;

  @Test
  public void shouldSaveAndRetrieveEntityById() throws InterruptedException {

    final Trace expected =
        TraceConverter.convertTraceToDao(TraceHelper.randomTrace(5));

    this.repository.insert(expected).await().indefinitely();

    final Trace actual =
        this.repository.getByTraceId(expected.getLandscapeToken(), expected.getTraceId())
            .collectItems().first().await().indefinitely();

    Assertions.assertEquals(expected, actual);
  }

  @Test
  public void shouldSaveAndRetrieveMultipleEntitiesById() throws InterruptedException {

    final Trace expected1 =
        TraceConverter.convertTraceToDao(TraceHelper.randomTrace(5));
    final Trace expected2 =
        TraceConverter.convertTraceToDao(TraceHelper.randomTrace(5));
    final Trace expected3 =
        TraceConverter.convertTraceToDao(TraceHelper.randomTrace(5));
    final Trace expected4 =
        TraceConverter.convertTraceToDao(TraceHelper.randomTrace(5));
    final Trace expected5 =
        TraceConverter.convertTraceToDao(TraceHelper.randomTrace(5));

    this.repository.insert(expected1).await().indefinitely();
    this.repository.insert(expected2).await().indefinitely();
    this.repository.insert(expected3).await().indefinitely();
    this.repository.insert(expected4).await().indefinitely();
    this.repository.insert(expected5).await().indefinitely();

    final Trace actual1 =
        this.repository.getByTraceId(expected1.getLandscapeToken(), expected1.getTraceId())
            .collectItems().first().await().indefinitely();

    final Trace actual2 =
        this.repository.getByTraceId(expected2.getLandscapeToken(), expected2.getTraceId())
            .collectItems().first().await().indefinitely();

    final Trace actual3 =
        this.repository.getByTraceId(expected3.getLandscapeToken(), expected3.getTraceId())
            .collectItems().first().await().indefinitely();

    final Trace actual4 =
        this.repository.getByTraceId(expected4.getLandscapeToken(), expected4.getTraceId())
            .collectItems().first().await().indefinitely();

    final Trace actual5 =
        this.repository.getByTraceId(expected5.getLandscapeToken(), expected5.getTraceId())
            .collectItems().first().await().indefinitely();

    Assertions.assertEquals(expected1, actual1);
    Assertions.assertEquals(expected2, actual2);
    Assertions.assertEquals(expected3, actual3);
    Assertions.assertEquals(expected4, actual4);
    Assertions.assertEquals(expected5, actual5);
  }

  @Test
  public void shouldSaveAndRetrieveAllEntities() throws InterruptedException {

    final String landscapeToken = RandomStringUtils.random(32, true, true);

    final Trace expected1 =
        TraceConverter.convertTraceToDao(TraceHelper.randomTrace(5, landscapeToken));
    final Trace expected2 =
        TraceConverter.convertTraceToDao(TraceHelper.randomTrace(5, landscapeToken));
    final Trace expected3 =
        TraceConverter.convertTraceToDao(TraceHelper.randomTrace(5, landscapeToken));
    final Trace expected4 =
        TraceConverter.convertTraceToDao(TraceHelper.randomTrace(5, landscapeToken));
    final Trace expected5 =
        TraceConverter.convertTraceToDao(TraceHelper.randomTrace(5, landscapeToken));

    final List<Trace> expectedList = new ArrayList<>();
    expectedList.add(expected1);
    expectedList.add(expected2);
    expectedList.add(expected3);
    expectedList.add(expected4);
    expectedList.add(expected5);

    this.repository.insert(expected1).await().indefinitely();
    this.repository.insert(expected2).await().indefinitely();
    this.repository.insert(expected3).await().indefinitely();
    this.repository.insert(expected4).await().indefinitely();
    this.repository.insert(expected5).await().indefinitely();

    final List<Trace> actualTraceList =
        this.repository.getAllAsync(landscapeToken).collectItems().asList().await().indefinitely();

    Assertions.assertTrue(expectedList.size() == actualTraceList.size());

    Assertions.assertTrue(actualTraceList.contains(expected1));
    Assertions.assertTrue(actualTraceList.contains(expected2));
    Assertions.assertTrue(actualTraceList.contains(expected3));
    Assertions.assertTrue(actualTraceList.contains(expected4));
    Assertions.assertTrue(actualTraceList.contains(expected5));
  }

}
