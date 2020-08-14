package net.explorviz.trace.persistence.cassandra;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.QueryExecutionException;
import com.datastax.oss.driver.api.core.servererrors.QueryValidationException;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import net.explorviz.avro.SpanDynamic;
import net.explorviz.avro.Trace;
import net.explorviz.trace.persistence.PersistingException;
import net.explorviz.trace.persistence.SpanRepository;
import net.explorviz.trace.service.TimestampHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cassandra-backed repository to access and save {@link net.explorviz.avro.Trace} entities.
 */
@ApplicationScoped
public class CassandraSpanRepository implements SpanRepository {

  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraSpanRepository.class);

  private final DBHelper db;

  /**
   * Create a new repository for accessing {@link Trace} object.
   *
   * @param db the backing Casandra db
   */
  @Inject
  public CassandraSpanRepository(final DBHelper db) {
    this.db = db;
    db.initialize();
  }



  @Override
  public void insert(SpanDynamic span) throws PersistingException {


    try {
      // Try to append span to the set of spans corresponding the trace id and token
      // This fails if the row does not exists
      if (!appendSpan(span)) {
        // Row for this trace id and token does not exist, create it with the single span
        createNew(span);
      }
    } catch (final AllNodesFailedException e) {
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error("Failed to insert new record: Database unreachable");
      }
      throw new PersistingException(e);
    } catch (QueryExecutionException | QueryValidationException e) {
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error("Failed to insert new record: {0}", e.getCause());
      }
      throw new PersistingException(e);
    }
  }


  /**
   * Creates a new entry.
   *
   * @param span the span to add
   */
  private void createNew(SpanDynamic span) {

    long timestamp = TimestampHelper.toInstant(span.getStartTime()).toEpochMilli();
    final SimpleStatement insertStmt =
        QueryBuilder.insertInto(DBHelper.KEYSPACE_NAME, DBHelper.TABLE_SPANS)
            .value(DBHelper.COL_TOKEN, QueryBuilder.literal(span.getLandscapeToken()))
            .value(DBHelper.COL_TIMESTAMP, QueryBuilder.literal(timestamp))
            .value(DBHelper.COL_TRACE_ID, QueryBuilder.literal(span.getTraceId()))
            .value(DBHelper.COL_SPANS, QueryBuilder.literal(Set.of(span), db.getCodecRegistry()))
            .build();
    this.db.getSession().execute(insertStmt);
  }

  /**
   * Appends a span to the (existing) set of all spans of the very trace id and landscape token
   *
   * @param span span to append to set of spans with the same trace id
   * @return true iff the update was successful, false if the corresponding row does not exists
   */
  private boolean appendSpan(SpanDynamic span) {
    final SimpleStatement updateStmt =
        QueryBuilder.update(DBHelper.KEYSPACE_NAME, DBHelper.TABLE_SPANS)
            .appendSetElement(DBHelper.COL_SPANS, QueryBuilder.literal(span, db.getCodecRegistry()))
            .whereColumn(DBHelper.COL_TOKEN)
            .isEqualTo(QueryBuilder.literal(span.getLandscapeToken()))
            .whereColumn(DBHelper.COL_TRACE_ID).isEqualTo(QueryBuilder.literal(span.getTraceId()))
            .ifExists()
            .build();

    return this.db.getSession().execute(updateStmt).wasApplied();

  }

  @Override
  public Optional<Collection<SpanDynamic>> getSpans(final String landscapeToken,
                                                    final String traceId) {

    SimpleStatement findStmt = QueryBuilder.selectFrom(DBHelper.KEYSPACE_NAME, DBHelper.TABLE_SPANS)
        .column(DBHelper.COL_SPANS)
        .whereColumn(DBHelper.COL_TOKEN).isEqualTo(QueryBuilder.literal(landscapeToken))
        .whereColumn(DBHelper.COL_TRACE_ID).isEqualTo(QueryBuilder.literal(traceId))
        .build().setTracing(true);
    ResultSet queryResult = this.db.getSession().execute(findStmt);
    ExecutionInfo exInfo = queryResult.getExecutionInfo();

    Row result = queryResult.one();
    if (result == null) {
      return Optional.empty();
    } else {
      Set<SpanDynamic> spans = result.getSet(DBHelper.COL_SPANS, SpanDynamic.class);
      exInfo.getQueryTraceAsync().thenAccept(t -> {
        Duration d = Duration.of(t.getDurationMicros(), ChronoUnit.MICROS);
        LOGGER.info("Fetched {} spans in {}ms", spans.size(), d.toMillis());
      });
      return Optional.ofNullable(spans);
    }
  }

  @Override
  public List<Set<SpanDynamic>> getAllInRange(String landscapeToken, final Instant from,
                                              final Instant to) {
    SimpleStatement findAllStmt =
        QueryBuilder.selectFrom(DBHelper.KEYSPACE_NAME, DBHelper.TABLE_SPANS)
            .column(DBHelper.COL_SPANS)
            .whereColumn(DBHelper.COL_TOKEN).isEqualTo(QueryBuilder.literal(landscapeToken))
            .whereColumn(DBHelper.COL_TIMESTAMP).isGreaterThanOrEqualTo(QueryBuilder.literal(from))
            .whereColumn(DBHelper.COL_TIMESTAMP).isLessThanOrEqualTo(QueryBuilder.literal(to))
            .build();
    ResultSet queryResults = this.db.getSession().execute(findAllStmt);
    ExecutionInfo exInfo = queryResults.getExecutionInfo();

    List<Set<SpanDynamic>> traces = new ArrayList<>();
    queryResults.forEach(r -> {
      Optional<Set<SpanDynamic>> possibleSpans =
          Optional.ofNullable(r.getSet(DBHelper.COL_SPANS, SpanDynamic.class));
      possibleSpans.ifPresent(traces::add);
    });
    exInfo.getQueryTraceAsync().thenAccept(t -> {
      Duration d = Duration.of(t.getDurationMicros(), ChronoUnit.MICROS);
      long count = traces.stream().mapToInt(Set::size).count();
      LOGGER.info("Fetched {} spans ({} traces) in {}ms", count, traces.size(), d.toMillis());
    });
    return traces;
  }


}
