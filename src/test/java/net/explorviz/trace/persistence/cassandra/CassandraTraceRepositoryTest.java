package net.explorviz.trace.persistence.cassandra;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import java.util.List;
import net.explorviz.avro.Trace;
import net.explorviz.trace.TraceHelper;
import net.explorviz.trace.persistence.PersistingException;
import net.explorviz.trace.persistence.cassandra.mapper.TraceMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CassandraTraceRepositoryTest extends CassandraTest {


  private CassandraTraceRepository repo;
  private TraceMapper mapper;

  @BeforeEach
  void setUp() {
    mapper = new TraceMapper(this.db);
    repo = new CassandraTraceRepository(this.db, mapper);
  }

  @AfterEach
  void tearDown() {
  }

  @Test
  void insert() throws PersistingException {
    Trace testTrace = TraceHelper.randomTrace(1);
    repo.insert(testTrace);

    String getInserted = QueryBuilder.selectFrom(DBHelper.KEYSPACE_NAME, DBHelper.TRACES_TABLE_NAME)
        .all()
        .whereColumn(DBHelper.COL_TOKEN)
        .isEqualTo(QueryBuilder.literal(testTrace.getLandscapeToken()))
        .whereColumn(DBHelper.COL_TRACE_ID).isEqualTo(QueryBuilder.literal(testTrace.getTraceId()))
        .asCql();
    List<Row> results = db.getSession().execute(getInserted).all();

    Assertions.assertEquals(1, results.size(), "Trace was not inserted");
  }

  @Test
  void insertValuesMatch() throws PersistingException {
    Trace testTrace = TraceHelper.randomTrace(1);
    repo.insert(testTrace);

    String getInserted = QueryBuilder.selectFrom(DBHelper.KEYSPACE_NAME, DBHelper.TRACES_TABLE_NAME)
        .all()
        .whereColumn(DBHelper.COL_TOKEN)
        .isEqualTo(QueryBuilder.literal(testTrace.getLandscapeToken()))
        .whereColumn(DBHelper.COL_TRACE_ID).isEqualTo(QueryBuilder.literal(testTrace.getTraceId()))
        .asCql();
    List<Row> results = db.getSession().execute(getInserted).all();

    Trace got = mapper.fromRow(results.get(0));
    Assertions.assertEquals(testTrace, got);

  }
}
