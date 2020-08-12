package net.explorviz.trace.persistence.cassandra;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import java.util.ArrayList;
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
    repo = new CassandraSpanRepository(this.db);
  }

  @AfterEach
  void tearDown() {
  }

  @Test
  void insertSingle() throws PersistingException {
    SpanDynamic testSpan = TraceHelper.randomSpan();
    repo.insert(testSpan);

    String getInserted = QueryBuilder.selectFrom(DBHelper.KEYSPACE_NAME, DBHelper.TABLE_SPANS)
        .all()
        .whereColumn(DBHelper.COL_TOKEN)
        .isEqualTo(QueryBuilder.literal(testSpan.getLandscapeToken()))
        .whereColumn(DBHelper.COL_TRACE_ID).isEqualTo(QueryBuilder.literal(testSpan.getTraceId()))
        .asCql();
    List<Row> results = db.getSession().execute(getInserted).all();

    testSpan.setLandscapeToken(""); // Not persisted, do not compare

    Set<SpanDynamic> got = results.get(0).getSet(DBHelper.COL_SPANS, SpanDynamic.class);
    Assertions.assertEquals(1, got.size());
    Assertions.assertEquals(testSpan, got.stream().findAny().get());
  }

  @Test
  void insertSingleTrace() throws PersistingException {
    final int spansPerTrace = 20;
    Trace testTrace = TraceHelper.randomTrace(spansPerTrace);

    for (SpanDynamic s : testTrace.getSpanList()) {
      repo.insert(s);
    }

    String getInserted = QueryBuilder.selectFrom(DBHelper.KEYSPACE_NAME, DBHelper.TABLE_SPANS)
        .all()
        .whereColumn(DBHelper.COL_TOKEN)
        .isEqualTo(QueryBuilder.literal(testTrace.getLandscapeToken()))
        .whereColumn(DBHelper.COL_TRACE_ID).isEqualTo(QueryBuilder.literal(testTrace.getTraceId()))
        .asCql();
    List<Row> results = db.getSession().execute(getInserted).all();

    // Only one trace was created
    Assertions.assertEquals(1, results.size());
    Set<SpanDynamic> got = results.get(0).getSet(DBHelper.COL_SPANS, SpanDynamic.class);

    List<SpanDynamic> expected = new ArrayList<>(testTrace.getSpanList());
    expected.sort((i,j) -> StringUtils.compare(i.getSpanId(), j.getSpanId()));
    expected.forEach(s -> s.setLandscapeToken("")); // Not persisted, do not compare

    List<SpanDynamic> gotList = new ArrayList<>(got);
    gotList.sort((i,j) -> StringUtils.compare(i.getSpanId(), j.getSpanId()));


    Assertions.assertEquals(expected, gotList);
  }

  @Test
  void insertMultipleTraces() throws PersistingException {
    final int spansPerTraces = 20;
    final int traceAmount = 20;

    for (int i = 0; i<traceAmount; i++) {
      Trace testTrace = TraceHelper.randomTrace(spansPerTraces);
      for (SpanDynamic s : testTrace.getSpanList()) {
        repo.insert(s);
      }
    }

    String getInserted = QueryBuilder.selectFrom(DBHelper.KEYSPACE_NAME, DBHelper.TABLE_SPANS)
        .all()
        .asCql();
    List<Row> results = db.getSession().execute(getInserted).all();

    // Exactly #traceAmount traces should be in persisted
    Assertions.assertEquals(traceAmount, results.size());

    // Each trace should have #spansPerTraces spans
    for (Row r: results) {
      Set<SpanDynamic> spans = r.getSet(DBHelper.COL_SPANS, SpanDynamic.class);
      Assertions.assertEquals(spansPerTraces, spans.size());
    }



  }
}
