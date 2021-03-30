package net.explorviz.trace.cassandra;

import com.datastax.oss.quarkus.test.CassandraTestResource;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@QuarkusTest
@QuarkusTestResource(KafkaTestResource.class)
@QuarkusTestResource(CassandraTestResource.class)
@TestProfile(CassandraTestProfile.class)
public class TraceResourceIt {

  private static final Logger LOGGER = LoggerFactory.getLogger(TraceResourceIt.class);

  @Inject
  TraceRepository repository;

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

  @Test
  public void shouldSaveAndRetrieveEntitiesByTimestamp() throws InterruptedException {

    final String landscapeToken = RandomStringUtils.random(32, true, true);

    final long fromSeconds1 = 1605700800L;
    final long toSeconds1 = 1605700810L;

    final long fromSeconds2 = 1605700811L;
    final long toSeconds2 = 1605700821L;

    final Trace expected1 =
        TraceConverter.convertTraceToDao(
            TraceHelper.randomTrace(5, landscapeToken, fromSeconds1, toSeconds1));
    final Trace expected2 =
        TraceConverter.convertTraceToDao(
            TraceHelper.randomTrace(5, landscapeToken, fromSeconds1, toSeconds1));
    final Trace expected3 =
        TraceConverter.convertTraceToDao(
            TraceHelper.randomTrace(5, landscapeToken, fromSeconds1, toSeconds1));
    final Trace remainder4 =
        TraceConverter.convertTraceToDao(
            TraceHelper.randomTrace(5, landscapeToken, fromSeconds2, toSeconds2));
    final Trace remainder5 =
        TraceConverter.convertTraceToDao(
            TraceHelper.randomTrace(5, landscapeToken, fromSeconds2, toSeconds2));

    long filteringKey =
        expected1.getStartTime() <= expected2.getStartTime() ? expected1.getStartTime()
            : expected2.getStartTime();

    filteringKey =
        filteringKey <= expected3.getStartTime() ? filteringKey : expected3.getStartTime();

    final List<Trace> expectedList = new ArrayList<>();
    expectedList.add(expected1);
    expectedList.add(expected2);
    expectedList.add(expected3);

    this.repository.insert(expected1).await().indefinitely();
    this.repository.insert(expected2).await().indefinitely();
    this.repository.insert(expected3).await().indefinitely();
    this.repository.insert(remainder4).await().indefinitely();
    this.repository.insert(remainder5).await().indefinitely();

    final List<Trace> actualTraceList =
        this.repository.getByStartTimeAndEndTime(landscapeToken, filteringKey, filteringKey + 1000)
            .collectItems().asList().await().indefinitely();

    Assertions.assertTrue(expectedList.size() == actualTraceList.size());

    Assertions.assertTrue(actualTraceList.contains(expected1));
    Assertions.assertTrue(actualTraceList.contains(expected2));
    Assertions.assertTrue(actualTraceList.contains(expected3));
  }

  @Test
  public void cloneToken() throws InterruptedException {

    final String landscapeToken = RandomStringUtils.random(32, true, true);

    final long fromSeconds1 = 1605700800L;
    final long toSeconds1 = 1605700810L;

    final Trace expected1 =
        TraceConverter.convertTraceToDao(
            TraceHelper.randomTrace(5, landscapeToken, fromSeconds1, toSeconds1));
    final Trace expected2 =
        TraceConverter.convertTraceToDao(
            TraceHelper.randomTrace(5, landscapeToken, fromSeconds1, toSeconds1));
    final Trace expected3 =
        TraceConverter.convertTraceToDao(
            TraceHelper.randomTrace(5, landscapeToken, fromSeconds1, toSeconds1));

    final List<Trace> expectedList = new ArrayList<>();
    expectedList.add(expected1);
    expectedList.add(expected2);
    expectedList.add(expected3);

    this.repository.insert(expected1).await().indefinitely();
    this.repository.insert(expected2).await().indefinitely();
    this.repository.insert(expected3).await().indefinitely();

    final String newLandscapeToken = RandomStringUtils.random(32, true, true);
    var newToken = this.repository.getAllAsync(newLandscapeToken).collectItems().asList().await().indefinitely();
    var clonedToken = this.repository.getAllAsync(landscapeToken).collectItems().asList().await().indefinitely();

    Assertions.assertEquals(0, newToken.size());
    Assertions.assertEquals(3, clonedToken.size());

    this.repository.cloneAllAsync(newLandscapeToken, landscapeToken).collectItems().asList().await().indefinitely();

    var result = this.repository.getAllAsync(newLandscapeToken).collectItems().asList().await().indefinitely();

    Assertions.assertEquals(3, result.size());
  }


}
