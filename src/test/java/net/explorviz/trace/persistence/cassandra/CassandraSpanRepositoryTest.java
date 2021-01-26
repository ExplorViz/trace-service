package net.explorviz.trace.persistence.cassandra;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import net.explorviz.avro.SpanDynamic;
import net.explorviz.avro.Trace;
import net.explorviz.trace.TraceHelper;
import net.explorviz.trace.persistence.PersistingException;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CassandraSpanRepositoryTest extends CassandraTest {


  private CassandraSpanRepository repo;


  @BeforeEach
  void setUp() {
    this.repo = new CassandraSpanRepository(this.db);
  }

  @Override
  @AfterEach
  void tearDown() {}

  @Test
  void insertSingle() throws PersistingException {
    final SpanDynamic testSpan = TraceHelper.randomSpan();
    this.repo.insert(testSpan);

    final String getInserted = QueryBuilder.selectFrom(DbHelper.KEYSPACE_NAME, DbHelper.TABLE_SPANS)
        .all()
        .whereColumn(DbHelper.COL_TOKEN)
        .isEqualTo(QueryBuilder.literal(testSpan.getLandscapeToken()))
        .whereColumn(DbHelper.COL_TRACE_ID).isEqualTo(QueryBuilder.literal(testSpan.getTraceId()))
        .asCql();
    final List<Row> results = this.db.getSession().execute(getInserted).all();

    testSpan.setLandscapeToken(""); // Not persisted, do not compare

    final Set<SpanDynamic> got = results.get(0).getSet(DbHelper.COL_SPANS, SpanDynamic.class);
    Assertions.assertEquals(1, got.size());
    Assertions.assertEquals(testSpan, got.stream().findAny().get());
  }

  @Test
  void saveTraceAsync() throws PersistingException {
    final Trace t = TraceHelper.randomTrace(20);

    this.repo.saveTraceAsync(t).toCompletableFuture().join();
    final Collection<SpanDynamic> got =
        this.repo.getSpans(t.getLandscapeToken(), t.getTraceId()).orElseThrow();
    Assertions.assertEquals(20, got.size());
  }

  @Test
  void insertSingleTrace() throws PersistingException {
    final int spansPerTrace = 20;
    final Trace testTrace = TraceHelper.randomTrace(spansPerTrace);

    for (final SpanDynamic s : testTrace.getSpanList()) {
      this.repo.insert(s);
    }

    final String getInserted = QueryBuilder.selectFrom(DbHelper.KEYSPACE_NAME, DbHelper.TABLE_SPANS)
        .all()
        .whereColumn(DbHelper.COL_TOKEN)
        .isEqualTo(QueryBuilder.literal(testTrace.getLandscapeToken()))
        .whereColumn(DbHelper.COL_TRACE_ID).isEqualTo(QueryBuilder.literal(testTrace.getTraceId()))
        .asCql();
    final List<Row> results = this.db.getSession().execute(getInserted).all();

    // Only one trace was created
    Assertions.assertEquals(1, results.size());
    final Set<SpanDynamic> got = results.get(0).getSet(DbHelper.COL_SPANS, SpanDynamic.class);

    final List<SpanDynamic> expected = new ArrayList<>(testTrace.getSpanList());
    expected.sort((i, j) -> StringUtils.compare(i.getSpanId(), j.getSpanId()));
    expected.forEach(s -> s.setLandscapeToken("")); // Not persisted, do not compare

    final List<SpanDynamic> gotList = new ArrayList<>(got);
    gotList.sort((i, j) -> StringUtils.compare(i.getSpanId(), j.getSpanId()));


    Assertions.assertEquals(expected, gotList);
  }


  @Test
  void insertMultipleTraces() throws PersistingException {
    final int spansPerTraces = 20;
    final int traceAmount = 20;

    for (int i = 0; i < traceAmount; i++) {
      final Trace testTrace = TraceHelper.randomTrace(spansPerTraces);
      for (final SpanDynamic s : testTrace.getSpanList()) {
        this.repo.insert(s);
      }
    }

    final String getInserted = QueryBuilder.selectFrom(DbHelper.KEYSPACE_NAME, DbHelper.TABLE_SPANS)
        .all()
        .asCql();
    final List<Row> results = this.db.getSession().execute(getInserted).all();

    // Exactly #traceAmount traces should be in persisted
    Assertions.assertEquals(traceAmount, results.size());

    // Each trace should have #spansPerTraces spans
    for (final Row r : results) {
      final Set<SpanDynamic> spans = r.getSet(DbHelper.COL_SPANS, SpanDynamic.class);
      Assertions.assertEquals(spansPerTraces, spans.size());
    }
  }

  @Test
  void findSpans() throws PersistingException {
    final int spansPerTraces = 20;

    final Trace testTrace = TraceHelper.randomTrace(spansPerTraces);
    for (final SpanDynamic s : testTrace.getSpanList()) {
      this.repo.insert(s);
    }

    final List<SpanDynamic> got =
        new ArrayList<>(
            this.repo.getSpans(testTrace.getLandscapeToken(), testTrace.getTraceId())
                .orElseThrow());


    final List<SpanDynamic> expected = testTrace.getSpanList();
    expected.forEach(s -> s.setLandscapeToken(""));
    expected.sort((i, j) -> StringUtils.compare(i.getSpanId(), j.getSpanId()));
    got.sort((i, j) -> StringUtils.compare(i.getSpanId(), j.getSpanId()));
    Assertions.assertEquals(expected, got);
  }

  @Test
  void deleteById() {

    final Trace testTrace = TraceHelper.randomTrace(100);
    final String token = testTrace.getLandscapeToken();
    for (final SpanDynamic s : testTrace.getSpanList()) {
      this.repo.insert(s);
    }
    this.repo.deleteAll(token);

    Assertions.assertFalse(this.repo.getSpans(token, testTrace.getTraceId()).isPresent());

  }

}
